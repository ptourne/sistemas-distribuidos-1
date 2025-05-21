package ratings

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/ptourne/sistemas-distribuidos-1/common/logger"
	"github.com/ptourne/sistemas-distribuidos-1/common/model"
	"github.com/ptourne/sistemas-distribuidos-1/middleware/middleware"
	"github.com/ptourne/sistemas-distribuidos-1/worker/task"
)

type JoinerRatings struct {
	inputToProcess      task.Task[*model.Row, *model.Row]
	inputToSave         task.Task[*model.Row, *model.Row]
	taskReceiverRatings middleware.Receiver[*model.Row]
	taskReceiverMovies  middleware.Receiver[*model.Row]
	taskSender          middleware.Sender[*model.Row]
	ratingsProcessed    int
	pendingMovies       map[string]map[string]*model.Row
	pendingMoviesMu     sync.Mutex
	subscribers         []string
	finishedRatings     map[string]bool
	id                  string
	log                 *logger.ConsoleLogger
}

func NewJoinerRatings(inputToProcess task.Task[*model.Row, *model.Row], inputToSave task.Task[*model.Row, *model.Row], subscribers []string, id string, log *logger.ConsoleLogger) task.JoinerTask[*model.Row, *model.Row] {
	joiner := JoinerRatings{inputToProcess, inputToSave, nil, nil, nil, 0, make(map[string]map[string]*model.Row), sync.Mutex{}, subscribers, make(map[string]bool), id, log}
	return &joiner
}

func (f *JoinerRatings) Input() string {
	return f.inputToProcess.Name()
}

func (f *JoinerRatings) Name() string {
	return "joiner_ratings"
}

func (f *JoinerRatings) NameWithId() string {
	return fmt.Sprintf("joiner_%s_ratings", f.id)
}

func (f *JoinerRatings) ProcessAndSend(env middleware.Envelope[*model.Row]) error {
	var err error
	row := env.Msg()
	row.Strings["cid"] = env.Cid()
	if title, ok := row.Strings["title"]; ok && title != "" {
		err = f.processMovieAndSendRatings(row)
	} else if _, ok := row.Floats["avg_rating"]; ok {
		err = f.processRating(row)
	} else {
		f.log.Warnf("Received row with no recognizable ID: %+v", row)
	}

	return err
}

func (f *JoinerRatings) processMovieAndSendRatings(row *model.Row) error {

	output, err := f.processMovie(row)
	if err != nil {
		//f.log.Errorf("Failed to process movie: %v", err)
		if err.Error() == "no rating found" {
			clientID := row.Strings["cid"]
			_, hasFinished := f.finishedRatings[clientID]
			if hasFinished {
				f.log.Infof("No rating found for movie %s", row.Strings["movieID"])
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
				f.log.Infof("Adding movie %s to pending movies", row.Strings["movieID"])
			} else {
				delete(f.pendingMovies[clientID], row.Strings["movieID"])
				f.log.Infof("No rating found for movie %s", row.Strings["movieID"])
			}
			f.pendingMoviesMu.Unlock()
			return nil
		}

		return err
	}
	if output == nil {
		return nil
	}
	return f.sendRating(output, row.Strings["cid"])
}

func (f *JoinerRatings) sendRating(output *model.Row, cid string) error {

	f.log.Infof("Sending rating data: %v", output)
	err := f.taskSender.Send(output, cid)
	if err != nil {
		f.log.Errorf("Failed to send rating data: %v", err)
		return err
	}

	return nil
}

func (f *JoinerRatings) getFilename(clientId string, movieID string) (string, error) {
	lastDigit := string(movieID[len(movieID)-1])
	dirPath := "joiner_ratings"

	if err := os.MkdirAll(dirPath, os.ModePerm); err != nil {
		f.log.Errorf("Failed to create directory: %s", "joiner_ratings")
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

	return fmt.Sprintf("%s/ratings_%s.csv", dirPath, lastDigit), nil
}

func (f *JoinerRatings) processRating(row *model.Row) error {
	f.ratingsProcessed++
	movieID := row.Strings["movieID"]
	avg_rating := row.Floats["avg_rating"]
	f.log.Infof("Processing rating %v : %v", f.ratingsProcessed, row)

	clientId := row.Strings["cid"]
	fileName, err := f.getFilename(clientId, movieID)
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
		f.log.Errorf("Failed to open file: %s. Err %s", fileName, err)
		return err
	}
	defer file.Close()
	writer := csv.NewWriter(file)
	defer writer.Flush()

	if writeHeader {
		if err := writer.Write([]string{"movieID", "rating"}); err != nil {
			f.log.Errorf("Failed to write CSV header: %v", err)
			return err
		}
	}

	ratingString := fmt.Sprintf("%f", avg_rating)

	err = writer.Write([]string{movieID, ratingString})
	if err != nil {
		f.log.Errorf("Failed to write CSV row: %v", err)
		return err
	}

	// ToDo: descomentar cuando este el reducer testeado
	clientId = row.Strings["cid"]
	found := false
	var movie *model.Row
	f.pendingMoviesMu.Lock()
	_, ok := f.pendingMovies[clientId]
	if ok {
		movie, found = f.pendingMovies[clientId][movieID]
		if found {
			//f.log.Infof("Found pending movie: %s in pending movies %v", movieID, f.pendingMovies)
			delete(f.pendingMovies[clientId], movieID)
		}
	}
	f.pendingMoviesMu.Unlock()

	if found {
		f.log.Infof("Processing pending movie: %s", movieID)
		rowRes := &model.Row{
			Strings: map[string]string{
				"movieID": movieID,
				"title":   movie.Strings["title"],
			},
			Floats: map[string]float64{

				"avg_rating": avg_rating,
			},
		}
		err = f.sendRating(rowRes, clientId)
		if err != nil {
			f.log.Errorf("Failed to process pending movie: %v", err)
		}
	}

	// if f.ratingsProcessed == 10000 { //TODO: sacar cuando se mergee con los cambios del reducer
	// 	f.notifyRatingsDone()
	// }

	return nil
}

func (f *JoinerRatings) processMovie(row *model.Row) (*model.Row, error) {
	movieID := row.Strings["movieID"]
	title := row.Strings["title"]
	f.log.Infof("Processing movie: %s", movieID)
	lastDigit := string(movieID[len(movieID)-1])
	clientId := row.Strings["cid"]

	dirPath := fmt.Sprintf("joiner_ratings/joiner%s/%s", f.id, clientId)

	fileName := fmt.Sprintf("%s/ratings_%s.csv", dirPath, lastDigit)
	file, err := os.Open(fileName)
	if err != nil {
		//f.log.Errorf("Failed to open file: %s", fileName)
		return nil, fmt.Errorf("no rating found")

	}
	defer file.Close()

	reader := csv.NewReader(file)
	_, err = reader.Read()
	if err != nil {
		f.log.Errorf("Failed to read header: %s", err)
		return nil, err
	}

	var avg_rating float64
	var found = false
	for {
		data, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil || len(data) < 2 {
			f.log.Errorf("Invalid ratings row: %v", err)
			continue
		}

		if data[0] == movieID {
			ratingString := data[1]
			rating, err := strconv.ParseFloat(ratingString, 64)
			if err != nil {
				f.log.Errorf("Failed to parse rating: %v; rating = %v", err, ratingString)
				continue

			}
			//f.log.Infof("Adding rating for movie %s, %f", movieID, rating)
			avg_rating = rating
			found = true
			break
		}
	}

	if !found {
		//f.log.Infof("No ratings found for movie %s", movieID)
		return nil, fmt.Errorf("no rating found")
	}
	//f.log.Infof("Average rating for movie %s: %f", movieID, avg)

	rowRes := &model.Row{
		Strings: map[string]string{
			"movieID": movieID,
			"title":   title,
		},
		Floats: map[string]float64{
			"avg_rating": avg_rating,
		},
	}

	return rowRes, nil

}

func (f *JoinerRatings) Connect(middlewareConnection middleware.Connection[*model.Row], _ middleware.Connection[*model.Row]) ([]chan middleware.Envelope[*model.Row], error) {
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

	var prefetchStr = os.Getenv("PREFETCH")
	if prefetchStr == "" {
		prefetchStr = "1"
	}
	prefetch, err := strconv.Atoi(prefetchStr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse PREFETCH: %w", err)
	}

	groupQueueName := f.NameWithId()
	f.taskReceiverRatings, err = middlewareConnection.ConsumeFrom(f.inputToSave.Name(), groupQueueName, uint(1), prefetch)
	if err != nil {
		return nil, fmt.Errorf("failed to create read queue clean_ratings for task %s", f.Name())
	}
	f.log.Infof("Created read queue exchange %s with groupName %s", f.Name(), groupQueueName)

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
	inputChannelRatings := make(chan middleware.Envelope[*model.Row])

	go func() {
		for {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			envelope, err := f.taskReceiverRatings.Next(ctx)
			if err != nil {
				if err.Error() == "read channel was closed" || err.Error() == "close channel was closed" {
					f.log.Infof("Channel for ratings closed from task: %v", f.Name())
					break
				}
				//f.log.Errorf("Error reading from middleware (joiner_ratings): %v", err)
				continue
			}
			// if !ok {
			// 	if envelope == nil || envelope.Type() != middleware.EOF {
			// 		f.log.Infof("Channel closed (ratings): %v", f.Name())

			// 		break
			// 	}
			// }
			inputChannelRatings <- envelope
		}
		//f.notifyRatingsDone()
		close(inputChannelRatings)
	}()

	go func() {
		for {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			envelope, err := f.taskReceiverMovies.Next(ctx)
			if err != nil {
				if err.Error() == "read channel was closed" || err.Error() == "close channel was closed" {
					f.log.Infof("Channel for movies closed from task: %v", f.Name())
					break
				}
				f.log.Errorf("Error reading from middleware: %v", err)
				continue
			}
			// if !ok {
			// 	if envelope == nil || envelope.Type() != middleware.EOF {
			// 		f.log.Infof("Channel closed (movies): %v", f.Name())

			// 		break
			// 	}
			// }
			inputChannelMovies <- envelope
		}
		close(inputChannelMovies)
	}()

	inputChannel := make([]chan middleware.Envelope[*model.Row], 2)
	inputChannel[0] = inputChannelMovies
	inputChannel[1] = inputChannelRatings
	return inputChannel, nil
}

func (f *JoinerRatings) Finish() error {
	if err := f.taskReceiverMovies.Close(); err != nil {
		return fmt.Errorf("failed to close task receiver (movies): %w", err)
	}
	if err := f.taskReceiverRatings.Close(); err != nil {
		return fmt.Errorf("failed to close task receiver (ratings): %w", err)
	}
	if err := f.taskSender.Close(); err != nil {
		return fmt.Errorf("failed to close task sender: %w", err)
	}
	f.log.Infof("Closed task %s", f.Name())
	return nil
}

func (f *JoinerRatings) ProcessPendingMovies(clientID string) error {
	f.log.Infof("Processing pending movies for client %s", clientID)
	f.pendingMoviesMu.Lock()
	pendings, ok := f.pendingMovies[clientID]
	if !ok {
		f.log.Infof("No pending movies for client %s", clientID)
		f.pendingMoviesMu.Unlock()
		return nil
	}
	f.pendingMoviesMu.Unlock()

	var err error
	for _, row := range pendings {
		err = f.processMovieAndSendRatings(row)
		if err != nil {
			return err
		}
	}
	f.pendingMoviesMu.Lock()
	delete(f.pendingMovies, clientID)
	f.pendingMoviesMu.Unlock()
	f.finishedRatings[clientID] = true
	return nil
}

func (f *JoinerRatings) FinishProcessingClient(clientID string, sendFinish bool) error {
	f.ProcessPendingMovies(clientID)
	dirPath := fmt.Sprintf("joiner_ratings/joiner%s/%s", f.id, clientID)
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

func (f *JoinerRatings) Id() string {
	return f.id
}

func (f *JoinerRatings) Logger() *logger.ConsoleLogger {
	return f.log
}
