package credits

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/ptourne/sistemas-distribuidos-1/common/logger"
	"github.com/ptourne/sistemas-distribuidos-1/common/model"
	cj "github.com/ptourne/sistemas-distribuidos-1/joiners_ratings_workers/commonJoiner"
	"github.com/ptourne/sistemas-distribuidos-1/middleware/middleware"
	"github.com/ptourne/sistemas-distribuidos-1/worker/task"
)

type JoinerCredits struct {
	inputToProcess      task.Task[*model.Row, *model.Row]
	inputToSave         task.Task[*model.Row, *model.Row]
	taskReceiverCredits middleware.Receiver[*model.Row]
	taskReceiverMovies  middleware.Receiver[*model.Row]
	taskSender          middleware.Sender[*model.Row]
	pendingMovies       map[string]map[string]*model.Row
	processedMovies     map[string]map[string]*model.Row
	subscribers         []string
	finishedCredits     map[string]bool
	msgIDs              map[string]uint64
	id                  string
	log                 *logger.ConsoleLogger
}

func NewJoinerCredits(inputToProcess task.Task[*model.Row, *model.Row], inputToSave task.Task[*model.Row, *model.Row], subscribers []string, id string, log *logger.ConsoleLogger) task.JoinerTask[*model.Row, *model.Row] {
	pendingMovies, processedMovies, msgIDs := cj.ReloadStateFromDisk("credits", id, []string{"movieID"}, []string{"movieID", "cast"}, log, cj.ReadCreditsCSVToMap, cj.WriteMovieID, cj.WriteCreditRow)
	joiner := JoinerCredits{inputToProcess, inputToSave, nil, nil, nil, pendingMovies, processedMovies, subscribers, make(map[string]bool), msgIDs, id, log}
	return &joiner
}

func (f *JoinerCredits) Input() string {
	return f.inputToProcess.Name()
}

func (f *JoinerCredits) Name() string {
	return "joiner_credits"
}

func (f *JoinerCredits) NameWithId() string {
	return fmt.Sprintf("joiner_%s_credits", f.id)
}

func (f *JoinerCredits) getCreditFilename(clientId string, movieID string) (string, error) {
	return f.getFileName(clientId, movieID, "credits")
}

func (f *JoinerCredits) getFileName(clientId string, dataID string, fileType string) (string, error) {
	dirPath := "joiner_credits"

	if err := os.MkdirAll(dirPath, os.ModePerm); err != nil {
		f.log.Errorf("Failed to create directory: %s", "joiner_credits")
		return "", err
	}

	dirPath = fmt.Sprintf("%s/joiner%s", dirPath, f.id)

	if err := os.MkdirAll(dirPath, os.ModePerm); err != nil {
		f.log.Errorf("Failed to create directory: %s", dirPath)
		return "", err
	}
	dirPath = fmt.Sprintf("%s/%s", dirPath, clientId)
	if err := os.MkdirAll(dirPath, os.ModePerm); err != nil {
		f.log.Errorf("Failed to create directory: %s", dirPath)
		return "", err
	}
	if fileType != "id" {
		lastDigit := string(dataID[len(dataID)-1])

		return fmt.Sprintf("%s/%s_%s.csv", dirPath, fileType, lastDigit), nil
	} else {
		return fmt.Sprintf("%s/id.txt", dirPath), nil
	}
}

func (f *JoinerCredits) getMoviesFilename(clientId string, movieID string) (string, error) {
	return f.getFileName(clientId, movieID, "movies")
}

func (f *JoinerCredits) getMsgIDFilename(clientId string) (string, error) {
	return f.getFileName(clientId, "", "id")
}

func (f *JoinerCredits) getProcessedMoviesFilename(clientId string, movieID string) (string, error) {
	return f.getFileName(clientId, movieID, "processed_movies")
}

func (f *JoinerCredits) ProcessAndSend(env middleware.Envelope[*model.Row]) error {
	var err error
	row := env.Msg()
	cid := env.Cid()
	if movieID, ok := row.Strings["movieID"]; ok && movieID != "" {
		err = f.processMovieAndSendActors(row, cid)
	} else if _, ok := row.Strings["ID"]; ok {
		err = f.processCredit(row, cid)
	} else {
		f.log.Warnf("Received row with no recognizable ID: %+v", row)
	}

	return err
}

func (f *JoinerCredits) wasProcessed(row *model.Row, clientID string) bool {
	movieID := row.Strings["movieID"]
	if _, exists := f.processedMovies[clientID]; exists {
		if _, wasProcessed := f.processedMovies[clientID][movieID]; wasProcessed {
			return true
		}
	}
	return false
}

func (f *JoinerCredits) processMovieAndSendActors(row *model.Row, clientID string) error {

	movieID := row.Strings["movieID"]
	isPending := false
	_, ok := f.pendingMovies[clientID]
	if ok {
		_, isPending = f.pendingMovies[clientID][movieID]
	}

	if !isPending && f.wasProcessed(row, clientID) {
		return nil
	}

	output, err := f.processMovie(row, clientID)
	if err != nil {
		if err.Error() == "no cast found" {
			_, hasFinished := f.finishedCredits[clientID]
			if hasFinished {
				f.log.Debugf("cid: %s | No cast found for movie %s", clientID, movieID)
				delete(f.pendingMovies[clientID], movieID)
				return nil
			}
			if !ok {
				f.pendingMovies[clientID] = make(map[string]*model.Row)
			}
			if !isPending {
				f.pendingMovies[clientID][movieID] = row
				f.SavePendingMovie(clientID, movieID)
				f.log.Debugf("cid: %s | Adding movie %s to pending movies", clientID, movieID)
			}
			// else {
			// 	delete(f.pendingMovies[clientID], movieID)
			// 	f.SaveProcessedMovie(clientID, movieID)
			// 	f.log.Infof("No cast found for movie %s", movieID)
			// }
			return nil
		}
		return err
	}
	err = f.sendActors(output, err, clientID)
	if err != nil {
		return err
	}
	f.SaveProcessedMovie(clientID, row.Strings["movieID"])
	return nil
}

func (f *JoinerCredits) sendActors(output []*model.Row, err error, cid string) error {
	if err != nil {
		return err
	}
	if len(output) == 0 {
		return nil
	}
	msgID, ok := f.msgIDs[cid]
	if !ok {
		msgID = 0
	}
	newMsgID := msgID + uint64(len(output))
	fileName, err := f.getMsgIDFilename(cid)
	if err != nil {
		return err
	}

	err = cj.SaveMsgId(fileName, newMsgID)
	if err != nil {
		f.log.Errorf("Failed to save message ID: %s", err)
		return err
	}

	for _, r := range output {
		if r == nil {
			continue
		}

		err = f.taskSender.Send(r, cid, msgID)
		if err != nil {
			f.log.Errorf("Failed to send actor data: %v", err)
			return err
		}
		msgID++
	}
	f.msgIDs[cid] = newMsgID
	return nil
}

func (f *JoinerCredits) processCredit(row *model.Row, clientId string) error {
	movieID := row.Strings["ID"]
	cast := row.Arrays["cast"]
	if len(cast) == 0 {
		return nil
	}
	f.log.Debugf("Processing credit: %s for client %s", movieID, clientId)
	fileName, err := f.getCreditFilename(clientId, movieID)
	if err != nil {
		f.log.Errorf("Failed to get filename: %s", err)
		return err
	}

	writeHeader := false
	if stat, err := os.Stat(fileName); err == nil {
		if stat.Size() == 0 {
			writeHeader = true
		}
	} else if os.IsNotExist(err) {
		writeHeader = true
	} else {
		f.log.Errorf("Error checking file status: %v", err)
		return err
	}

	file, err := os.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		f.log.Errorf("Failed to open file: %s", fileName)
		return err
	}
	defer file.Close()
	writer := csv.NewWriter(file)
	defer writer.Flush()

	if writeHeader {
		if err := writer.Write([]string{"movieID", "cast"}); err != nil {
			f.log.Errorf("Failed to write CSV header: %v", err)
			return err
		}
	}

	castString, err := json.Marshal(cast)
	//f.log.Infof("Writing cast for from strings=%v, arrays=%v", row.Strings, row.Arrays)
	if err != nil {
		f.log.Errorf("Failed to marshal cast: %v", err)
		return err
	}

	err = writer.Write([]string{movieID, string(castString)})
	if err != nil {
		f.log.Errorf("Failed to write CSV row: %v", err)
		return err
	}

	found := false
	_, ok := f.pendingMovies[clientId]
	if ok {
		_, found = f.pendingMovies[clientId][movieID]
		if found {
			f.log.Debugf("Processing pending movie: %s", movieID)
			flattenCast := flattenCastList(cast, movieID)
			err = f.sendActors(flattenCast, nil, clientId)
			if err != nil {
				f.log.Errorf("Failed to process pending movie: %v", err)
			} else {
				f.SaveProcessedMovie(clientId, movieID)
				delete(f.pendingMovies[clientId], movieID)
			}
		}
	}
	return nil
}

func (f *JoinerCredits) processMovie(row *model.Row, clientId string) ([]*model.Row, error) {
	var flattenCast []*model.Row
	movieID := row.Strings["movieID"]
	lastDigit := string(movieID[len(movieID)-1])

	f.log.Debugf("Processing movie: %s for client %s", movieID, clientId)
	dirPath := fmt.Sprintf("joiner_credits/joiner%s/%s", f.id, clientId)

	fileName := fmt.Sprintf("%s/credits_%s.csv", dirPath, lastDigit)
	file, err := os.Open(fileName)
	if err != nil {
		return nil, fmt.Errorf("no cast found") //err

	}
	defer file.Close()

	reader := csv.NewReader(file)
	_, err = reader.Read()
	if err != nil {
		f.log.Errorf("Failed to read header: %s", err)
		return nil, err
	}

	var cast string

	for {
		data, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil || len(data) < 2 {
			f.log.Errorf("Invalid credits row: %v", err)
			continue
		}

		if data[0] == movieID {
			cast = data[1]
			break
		}
	}

	if cast == "" || cast == "null" {
		return nil, fmt.Errorf("no cast found")
	}

	var castList []string
	if err := json.Unmarshal([]byte(cast), &castList); err != nil {
		f.log.Errorf("Failed to unmarshal cast: %v; cast = %v", err, cast)
		return nil, err
	}

	flattenCast = flattenCastList(castList, movieID)
	return flattenCast, nil
}

func (f *JoinerCredits) SavePendingMovie(clientID string, movieID string) error {
	fileName, err := f.getMoviesFilename(clientID, movieID)
	if err != nil {
		f.log.Errorf("Failed to get filename: %s", err)
		return err
	}

	return SaveMovie(clientID, movieID, fileName, f.log)
}

func (f *JoinerCredits) SaveProcessedMovie(clientID string, movieID string) error {
	if _, ok := f.processedMovies[clientID]; ok {
		if _, ok := f.processedMovies[clientID][movieID]; ok {
			return nil
		}
	} else {
		f.processedMovies[clientID] = make(map[string]*model.Row)
	}
	fileName, err := f.getProcessedMoviesFilename(clientID, movieID)
	if err != nil {
		f.log.Errorf("Failed to get filename: %s", err)
		return err
	}

	err = SaveMovie(clientID, movieID, fileName, f.log)
	if err != nil {
		return err
	}
	f.processedMovies[clientID][movieID] = &model.Row{Strings: map[string]string{"movieID": movieID}}
	return nil
}

func (f *JoinerCredits) Connect(middlewareConnection middleware.Connection[*model.Row], _ middleware.Connection[*model.Row]) ([]chan middleware.Envelope[*model.Row], error) {
	var err error
	var WORKER_COUNT_STR = os.Getenv("WORKER_COUNT")
	if WORKER_COUNT_STR == "" {
		f.log.Errorf("WORKER_COUNT environment variable not set. It will be set to 1")
		WORKER_COUNT_STR = "1"
	}
	peers, err := strconv.Atoi(WORKER_COUNT_STR)
	if err != nil {
		return nil, fmt.Errorf("failed to parse WORKER_COUNT: %w", err)
	}

	prefetch := 500
	groupQueueName := f.NameWithId()
	consumerCountStr := os.Getenv("CONSUMER_COUNT")
	if consumerCountStr == "" {
		consumerCountStr = "1"
	}
	consumerCount, err := strconv.Atoi(consumerCountStr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse CONSUMER_COUNT: %w", err)
	}
	f.taskReceiverCredits, err = middlewareConnection.ConsumeFrom(f.inputToSave.Name(), groupQueueName, "0", prefetch, uint(1))
	if err != nil {
		return nil, fmt.Errorf("failed to create read queue clean_credits for task %s", f.Name())
	}

	f.taskReceiverMovies, err = middlewareConnection.ConsumeFrom(f.Input(), f.Name(), f.id, prefetch, uint(peers))
	if err != nil {
		return nil, fmt.Errorf("failed to create read queue %s for task %s", f.Input(), f.Name())
	}

	f.taskSender, err = middlewareConnection.WriteTo(f.Name(), f.subscribers, f.id, uint(consumerCount))
	if err != nil {
		return nil, fmt.Errorf("failed to create write queue for task %s", f.Name())
	}

	//lint:ignore S1019 Ignorar reflect.Select en este archivo
	inputChannelMovies := make(chan middleware.Envelope[*model.Row], 0)
	inputChannelCredits := make(chan middleware.Envelope[*model.Row])

	go func() {
		for {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			envelope, err := f.taskReceiverCredits.Next(ctx)
			if err != nil {
				if err.Error() == "read channel was closed" || err.Error() == "close channel was closed" || err.Error() == "read channel is not initialized" {
					f.log.Infof("Channel for credits closed from task: %v", f.Name())
					break
				}
				continue
			}
			inputChannelCredits <- envelope
		}

		close(inputChannelCredits)
	}()

	go func() {

		for {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			envelope, err := f.taskReceiverMovies.Next(ctx)
			if err != nil {
				if err.Error() == "read channel was closed" || err.Error() == "close channel was closed" || err.Error() == "read channel is not initialized" {
					f.log.Infof("Channel for movies closed from task: %v", f.Name())
					break
				}
				continue
			}

			inputChannelMovies <- envelope
		}
		close(inputChannelMovies)
	}()

	inputChannel := make([]chan middleware.Envelope[*model.Row], 2)
	inputChannel[0] = inputChannelMovies
	inputChannel[1] = inputChannelCredits
	return inputChannel, nil
}

func (f *JoinerCredits) Finish() error {
	if err := f.taskReceiverMovies.Close(); err != nil {
		return fmt.Errorf("failed to close task receiver (movies): %w", err)
	}
	if err := f.taskReceiverCredits.Close(); err != nil {
		return fmt.Errorf("failed to close task receiver (credits): %w", err)
	}
	if err := f.taskSender.Close(); err != nil {
		return fmt.Errorf("failed to close task sender: %w", err)
	}
	f.log.Infof("Closed task %s", f.Name())
	return nil
}

func (f *JoinerCredits) ProcessPendingMovies(clientID string) error {
	f.log.Infof("Processing pending movies for client %s", clientID)
	f.finishedCredits[clientID] = true
	pendings, ok := f.pendingMovies[clientID]
	if !ok {
		f.log.Infof("No pending movies for client %s", clientID)
		return nil
	}

	var err error
	for _, row := range pendings {
		err2 := f.processMovieAndSendActors(row, clientID)
		if err2 != nil {
			err = err2
		}
		if !f.wasProcessed(row, clientID) {
			f.SaveProcessedMovie(clientID, row.Strings["movieID"])
		}
	}
	delete(f.pendingMovies, clientID)
	return err
}

func (f *JoinerCredits) FinishProcessingClient(clientID string, sendFinish bool) error {
	f.log.Infof("Finish processing credits and movies for client %s", clientID)
	f.ProcessPendingMovies(clientID)
	dirPath := fmt.Sprintf("joiner_credits/joiner%s/%s", f.id, clientID)
	err := os.RemoveAll(dirPath)
	if err != nil {
		return err
	} else {
		f.log.Infof("Removed directory: %s", dirPath)
	}
	if sendFinish {
		f.log.Infof("Sending EOF to client %s", clientID)
		f.taskSender.SendEOF(clientID)
	}
	return nil
}

func (f *JoinerCredits) Id() string {
	return f.id
}

func (f *JoinerCredits) Logger() *logger.ConsoleLogger {
	return f.log
}

func SaveMovie(clientID string, movieID string, fileName string, log *logger.ConsoleLogger) error {

	writeHeader := false
	if stat, err := os.Stat(fileName); err == nil {
		if stat.Size() == 0 {
			writeHeader = true
		}
	} else if os.IsNotExist(err) {
		writeHeader = true
	} else {
		log.Errorf("Error checking file status: %v", err)
		return err
	}

	file, err := os.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Errorf("Failed to open file: %s", fileName)
		return err
	}
	defer file.Close()
	writer := csv.NewWriter(file)
	defer writer.Flush()

	if writeHeader {
		if err := writer.Write([]string{"movieID"}); err != nil {
			log.Errorf("Failed to write CSV header: %v", err)
			return err
		}
	}

	err = writer.Write([]string{movieID})
	if err != nil {
		log.Errorf("Failed to write CSV row: %v", err)
		return err
	}
	writer.Flush()
	if err := writer.Error(); err != nil {
		log.Errorf("Flush error writing CSV: %v", err)
		return err
	}
	return nil
}

func flattenCastList(castList []string, movieID string) []*model.Row {
	var flattenCast []*model.Row

	for _, actor := range castList {
		actor = strings.Trim(actor, " ")
		if actor != "" {
			flattenCast = append(flattenCast, &model.Row{
				Strings: map[string]string{
					"movieID": movieID,
					"actor":   actor,
				},
			})
		}
	}
	return flattenCast
}
