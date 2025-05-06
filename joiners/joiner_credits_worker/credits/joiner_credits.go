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
	"sync"
	"time"

	"github.com/ptourne/sistemas-distribuidos-1/common/model"
	"github.com/ptourne/sistemas-distribuidos-1/middleware/middleware"
	"github.com/ptourne/sistemas-distribuidos-1/worker/task"
)

type JoinerCredits struct {
	inputToProcess      task.Task[*model.Row, *model.Row]
	inputToSave         task.Task[*model.Row, *model.Row]
	taskReceiverCredits middleware.Receiver[*model.Row]
	taskReceiverMovies  middleware.Receiver[*model.Row]
	taskSender          middleware.Sender[*model.Row]
	creditsProcessed    int
	pendingMovies       map[string]map[string]*model.Row
	pendingMoviesMu     sync.Mutex
	subscribers         []string
	finishedCredits     map[string]bool
}

func NewJoinerCredits(inputToProcess task.Task[*model.Row, *model.Row], inputToSave task.Task[*model.Row, *model.Row], subscribers []string) task.JoinerTask[*model.Row, *model.Row] {
	joiner := JoinerCredits{inputToProcess, inputToSave, nil, nil, nil, 0, make(map[string]map[string]*model.Row), sync.Mutex{}, subscribers, make(map[string]bool)}
	return &joiner
}

func (f *JoinerCredits) Input() string {
	return f.inputToProcess.Name()
}

func (f *JoinerCredits) Name() string {
	return "joiner_credits"
}

func (f *JoinerCredits) ProcessAndSend(env middleware.Envelope[*model.Row]) error {
	var err error
	row := env.Msg()
	row.Strings["cid"] = env.Cid()
	if movieID, ok := row.Strings["movieID"]; ok && movieID != "" {
		err = f.processMovieAndSendActors(row)
	} else if _, ok := row.Strings["ID"]; ok {
		err = f.processCredit(row)
	} else {
		Log.Warnf("Received row with no recognizable ID: %+v", row)
	}

	return err
}

func (f *JoinerCredits) processMovieAndSendActors(row *model.Row) error {

	output, err := f.processMovie(row)
	if err != nil {
		if err.Error() == "no cast found" {
			clientID := row.Strings["cid"]
			_, hasFinished := f.finishedCredits[clientID]
			if hasFinished {
				Log.Debugf("No cast found for movie %s", row.Strings["movieID"])
				delete(f.pendingMovies[clientID], row.Strings["movieID"])
				return nil
			}
			f.pendingMoviesMu.Lock()
			_, ok := f.pendingMovies[clientID]
			if !ok {
				f.pendingMovies[clientID] = make(map[string]*model.Row)
			}
			_, ok = f.pendingMovies[clientID][row.Strings["movieID"]]
			if !ok {
				f.pendingMovies[clientID][row.Strings["movieID"]] = row
				Log.Debugf("Adding movie %s to pending movies", row.Strings["movieID"])
			} else {
				delete(f.pendingMovies[clientID], row.Strings["movieID"])
				Log.Infof("No cast found for movie %s", row.Strings["movieID"])
			}
			f.pendingMoviesMu.Unlock()
			return nil
		}
		//log.Errorf("Failed to process movie: %v", err)
		return err
	}
	return f.sendActors(output, err, row.Strings["cid"])
}

func (f *JoinerCredits) sendActors(output []*model.Row, err error, cid string) error {
	if err != nil {
		//log.Errorf("Failed to process movie: %v", err)
		return err
	}
	if len(output) == 0 {
		return nil
	}
	for _, r := range output {
		if r == nil {
			continue
		}
		//Log.Infof("Sending actor %+v", r)
		err = f.taskSender.Send(r, cid)
		if err != nil {
			Log.Errorf("Failed to send actor data: %v", err)
			return err
		}
	}
	return nil
}

func (f *JoinerCredits) processCredit(row *model.Row) error {
	f.creditsProcessed++
	movieID := row.Strings["ID"]
	cast := row.Arrays["cast"]
	//log.Infof("Processing credit %v", f.creditsProcessed)
	// if f.creditsProcessed == 45476 && len(cast) == 0 { //TODO
	// 	f.processPendingMovies()
	// }
	if len(cast) == 0 {
		return nil
	}
	clientId := row.Strings["cid"]
	Log.Debugf("Processing credit: %s for client %s", movieID, clientId)
	lastDigit := string(movieID[len(movieID)-1])

	if err := os.MkdirAll(clientId, os.ModePerm); err != nil {
		Log.Errorf("Failed to create directory: %s", clientId)
		return err
	}
	dirPath := fmt.Sprintf("%s/joiner_credits", clientId)
	if err := os.MkdirAll(dirPath, os.ModePerm); err != nil {
		Log.Errorf("Failed to create directory: %s", "joiner_credits")
		return err
	}
	dirPath = fmt.Sprintf("%s/joiner%s", dirPath, WORKER_ID)
	if err := os.MkdirAll(dirPath, os.ModePerm); err != nil {
		Log.Errorf("Failed to create directory: %s", dirPath)
		return err
	}

	fileName := fmt.Sprintf("%s/credits_%s.csv", dirPath, lastDigit)

	writeHeader := false
	if stat, err := os.Stat(fileName); err == nil {
		if stat.Size() == 0 {
			writeHeader = true
		}
	} else if os.IsNotExist(err) {
		writeHeader = true
	} else {
		Log.Errorf("Error checking file status: %v", err)
		return err
	}

	file, err := os.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		Log.Errorf("Failed to open file: %s", fileName)
		return err
	}
	defer file.Close()
	writer := csv.NewWriter(file)
	defer writer.Flush()

	if writeHeader {
		if err := writer.Write([]string{"movieID", "cast"}); err != nil {
			Log.Errorf("Failed to write CSV header: %v", err)
			return err
		}
	}

	castString, err := json.Marshal(cast)
	//log.Infof("Writing cast for from strings=%v, arrays=%v", row.Strings, row.Arrays)
	if err != nil {
		Log.Errorf("Failed to marshal cast: %v", err)
		return err
	}

	err = writer.Write([]string{movieID, string(castString)})
	if err != nil {
		Log.Errorf("Failed to write CSV row: %v", err)
		return err
	}

	clientId = row.Strings["cid"]
	found := false
	f.pendingMoviesMu.Lock()
	_, ok := f.pendingMovies[clientId]
	if ok {
		_, found = f.pendingMovies[clientId][movieID]
		if found {
			//log.Infof("Found pending movie: %s in pending movies %v", movieID, f.pendingMovies)
			delete(f.pendingMovies[clientId], movieID)
		}
	}
	f.pendingMoviesMu.Unlock()
	if found {
		Log.Debugf("Processing pending movie: %s", movieID)
		flattenCast := flattenCastList(cast, movieID)
		err = f.sendActors(flattenCast, nil, clientId)
		if err != nil {
			Log.Errorf("Failed to process pending movie: %v", err)
		}
	}

	return nil
}

func (f *JoinerCredits) processMovie(row *model.Row) ([]*model.Row, error) {
	var flattenCast []*model.Row
	movieID := row.Strings["movieID"]
	lastDigit := string(movieID[len(movieID)-1])
	clientId := row.Strings["cid"]
	Log.Debugf("Processing movie: %s for client %s", movieID, clientId)
	dirPath := fmt.Sprintf("%s/joiner_credits/joiner%s", clientId, WORKER_ID)

	fileName := fmt.Sprintf("%s/credits_%s.csv", dirPath, lastDigit)
	file, err := os.Open(fileName)
	if err != nil {
		//log.Errorf("Failed to open file: %s", fileName)
		return nil, fmt.Errorf("no cast found") //err

	}
	defer file.Close()

	reader := csv.NewReader(file)
	_, err = reader.Read()
	if err != nil {
		Log.Errorf("Failed to read header: %s", err)
		return nil, err
	}

	var cast string

	for {
		data, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil || len(data) < 2 {
			Log.Errorf("Invalid credits row: %v", err)
			continue
		}

		if data[0] == movieID {
			//log.Infof("Found cast for %s", movieID)
			cast = data[1]
			break
		}
	}

	if cast == "" || cast == "null" {
		//log.Infof("No cast found for %s", movieID)
		return nil, fmt.Errorf("no cast found")
	}

	var castList []string
	if err := json.Unmarshal([]byte(cast), &castList); err != nil {
		Log.Errorf("Failed to unmarshal cast: %v; cast = %v", err, cast)
		return nil, err
	}

	flattenCast = flattenCastList(castList, movieID)
	return flattenCast, nil

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

func (f *JoinerCredits) Connect(middlewareConnection middleware.Connection[*model.Row], _ middleware.Connection[*model.Row]) ([]chan middleware.Envelope[*model.Row], error) {
	var err error
	id := WORKER_ID
	if id == "" {
		id = "1"
	}
	var WORKER_COUNT_STR = os.Getenv("WORKER_COUNT")
	if WORKER_COUNT_STR == "" {
		Log.Errorf("WORKER_COUNT environment variable not set. It will be set to 1")
		WORKER_COUNT_STR = "1"
	}
	peers, err := strconv.Atoi(WORKER_COUNT_STR)
	if err != nil {
		return nil, fmt.Errorf("failed to parse WORKER_COUNT: %w", err)
	}

	prefetchStr := os.Getenv("PREFETCH")
	if prefetchStr == "" {
		prefetchStr = "1"
	}
	prefetch, err := strconv.Atoi(prefetchStr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse PREFETCH: %w", err)
	}
	groupQueueName := fmt.Sprintf("joiner_%s_credits", id)
	f.taskReceiverCredits, err = middlewareConnection.ConsumeFrom(f.inputToSave.Name(), groupQueueName, uint(peers), prefetch)
	if err != nil {
		return nil, fmt.Errorf("failed to create read queue clean_credits for task %s", f.Name())
	}

	f.taskReceiverMovies, err = middlewareConnection.ConsumeFrom(f.Input(), f.Name(), uint(peers), prefetch)
	if err != nil {
		return nil, fmt.Errorf("failed to create read queue %s for task %s", f.Input(), f.Name())
	}

	f.taskSender, err = middlewareConnection.WriteTo(f.Name(), f.subscribers)
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
				if err.Error() == "read channel was closed" || err.Error() == "close channel was closed" {
					Log.Infof("Channel for credits closed from task: %v", f.Name())
					break
				}
				//Log.Errorf("Error reading from middleware (joiner_credits): %v", err)
				continue
			}
			// if !ok {
			// 	if envelope == nil || envelope.Type() != middleware.EOF {
			// 		Log.Infof("Channel closed (credits): %v", f.Name())

			// 		break
			// 	}
			// }
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
				if err.Error() == "read channel was closed" || err.Error() == "close channel was closed" {
					Log.Infof("Channel for movies closed from task: %v", f.Name())
					break
				}
				//Log.Errorf("Error reading from middleware: %v", err)
				continue
			}
			// if !ok {
			// 	if envelope == nil || envelope.Type() != middleware.EOF {
			// 		Log.Infof("Channel closed (movies): %v", f.Name())
			// 		break
			// 	}
			// }
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
	Log.Infof("Closed task %s", f.Name())
	return nil
}

func (f *JoinerCredits) ProcessPendingMovies(clientID string) error {
	Log.Infof("Processing pending movies for client %s", clientID)
	f.pendingMoviesMu.Lock()
	pendings, ok := f.pendingMovies[clientID]
	if !ok {
		Log.Infof("No pending movies for client %s", clientID)
		f.pendingMoviesMu.Unlock()
		return nil
	}
	f.pendingMoviesMu.Unlock()

	var err error
	for _, row := range pendings {
		err = f.processMovieAndSendActors(row)
		if err != nil {
			return err
		}
	}
	f.pendingMoviesMu.Lock()
	delete(f.pendingMovies, clientID)
	f.pendingMoviesMu.Unlock()
	f.finishedCredits[clientID] = true
	return nil
}

func (f *JoinerCredits) FinishProcessingClient(clientID string) error {
	Log.Infof("Finish processing credits and movies for client %s", clientID)
	f.ProcessPendingMovies(clientID)
	dirPath := fmt.Sprintf("%s/joiner_credits/joiner%s", clientID, WORKER_ID)
	err := os.RemoveAll(dirPath)
	if err != nil {
		return err
	} else {
		Log.Infof("Removed directory: %s", dirPath)
	}
	f.taskSender.SendEOF(clientID)
	return nil
}
