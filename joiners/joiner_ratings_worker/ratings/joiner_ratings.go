package ratings

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
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
	pendingMovies       map[string]map[string]*model.Row
	processedMovies     map[string]map[string]*model.Row
	subscribers         []string
	finishedRatings     map[string]bool
	id                  string
	log                 *logger.ConsoleLogger
}

func NewJoinerRatings(inputToProcess task.Task[*model.Row, *model.Row], inputToSave task.Task[*model.Row, *model.Row], subscribers []string, id string, log *logger.ConsoleLogger) task.JoinerTask[*model.Row, *model.Row] {
	pendingMovies, processedMovies := reloadPendingMoviesFromDisk(id, log)
	joiner := JoinerRatings{inputToProcess, inputToSave, nil, nil, nil, pendingMovies, processedMovies, subscribers, make(map[string]bool), id, log}
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
		err = f.processRating(row, env.Id())
	} else {
		f.log.Warnf("Received row with no recognizable ID: %+v", row)
	}

	return err
}

func (f *JoinerRatings) wasProcessed(row *model.Row) bool {
	clientID := row.Strings["cid"]
	movieID := row.Strings["movieID"]
	if _, exists := f.processedMovies[clientID]; exists {
		if _, wasProcessed := f.processedMovies[clientID][movieID]; wasProcessed {
			return true
		}
	}
	return false
}

func (f *JoinerRatings) processMovieAndSendRatings(row *model.Row) error {

	clientID := row.Strings["cid"]
	movieID := row.Strings["movieID"]
	isPending := false
	_, ok := f.pendingMovies[clientID]
	if ok {
		_, isPending = f.pendingMovies[clientID][movieID]
	}
	if !isPending && f.wasProcessed(row) {
		return nil
	}

	output, id, err := f.processMovie(row)
	if err != nil {
		if err.Error() == "no rating found" {
			_, hasFinished := f.finishedRatings[clientID]
			if hasFinished {
				f.log.Infof("No rating found for movie %s", movieID)
				delete(f.pendingMovies[clientID], movieID)
				return nil
			}
			if !ok {
				f.pendingMovies[clientID] = make(map[string]*model.Row)
			}
			if !isPending {
				f.pendingMovies[clientID][movieID] = row
				f.SavePendingMovie(clientID, movieID, row.Strings["title"])
				f.log.Infof("Adding movie %s to pending movies", movieID)
			}
			// else {
			// 	delete(f.pendingMovies[clientID], movieID)
			// 	f.SaveProcessedMovie(clientID, movieID, row.Strings["title"])
			// 	f.log.Infof("No rating found for movie %s", movieID)
			// }
			return nil
		}

		return err
	}
	if output == nil {
		return nil
	}
	err = f.sendRating(output, row.Strings["cid"], id)
	if err != nil {
		f.log.Errorf("Failed to send rating: %v", err)
		return err
	}

	return f.SaveProcessedMovie(clientID, row.Strings["movieID"], row.Strings["title"])
}

func (f *JoinerRatings) sendRating(output *model.Row, cid string, id uint64) error {

	f.log.Infof("Sending rating data: %v", output)
	err := f.taskSender.Send(output, cid, id)
	if err != nil {
		f.log.Errorf("Failed to send rating data: %v", err)
		return err
	}

	return nil
}

func (f *JoinerRatings) processRating(row *model.Row, id uint64) error {
	movieID := row.Strings["movieID"]
	avg_rating := row.Floats["avg_rating"]
	f.log.Infof("Processing rating %v :", row)

	clientId := row.Strings["cid"]
	fileName, err := f.getRatingFilename(clientId, movieID)
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
		if err := writer.Write([]string{"movieID", "rating", "id"}); err != nil {
			f.log.Errorf("Failed to write CSV header: %v", err)
			return err
		}
	}

	ratingString := fmt.Sprintf("%f", avg_rating)

	err = writer.Write([]string{movieID, ratingString, strconv.FormatUint(id, 10)})
	if err != nil {
		f.log.Errorf("Failed to write CSV row: %v", err)
		return err
	}

	// ToDo: descomentar cuando este el reducer testeado
	clientId = row.Strings["cid"]
	found := false
	var movie *model.Row
	_, ok := f.pendingMovies[clientId]
	if ok {
		movie, found = f.pendingMovies[clientId][movieID]
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
			err = f.sendRating(rowRes, clientId, id)
			if err != nil {
				f.log.Errorf("Failed to process pending movie: %v", err)
			} else {
				f.SaveProcessedMovie(clientId, movieID, movie.Strings["title"])
				delete(f.pendingMovies[clientId], movieID)
			}
		}
	}

	return nil
}

func (f *JoinerRatings) processMovie(row *model.Row) (*model.Row, uint64, error) {
	movieID := row.Strings["movieID"]
	title := row.Strings["title"]
	clientId := row.Strings["cid"]
	var id uint64

	f.log.Infof("Processing movie: %s", movieID)
	lastDigit := string(movieID[len(movieID)-1])

	dirPath := fmt.Sprintf("joiner_ratings/joiner%s/%s", f.id, clientId)

	fileName := fmt.Sprintf("%s/ratings_%s.csv", dirPath, lastDigit)
	file, err := os.Open(fileName)
	if err != nil {
		//f.log.Errorf("Failed to open file: %s", fileName)
		return nil, id, fmt.Errorf("no rating found")

	}
	defer file.Close()

	reader := csv.NewReader(file)
	_, err = reader.Read()
	if err != nil {
		f.log.Errorf("Failed to read header: %s", err)
		return nil, id, err
	}

	var avg_rating float64
	var found = false
	for {
		data, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil || len(data) < 3 {
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
			id, err = strconv.ParseUint(data[2], 10, 64)
			if err != nil {
				f.log.Errorf("Failed to parse ID: %v", err)
				return nil, id, fmt.Errorf("failed to parse ID: %w", err)
			}
			break
		}
	}

	if !found {
		//f.log.Infof("No ratings found for movie %s", movieID)
		return nil, id, fmt.Errorf("no rating found")
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

	return rowRes, id, nil

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
	consumerCountStr := os.Getenv("CONSUMER_COUNT")
	if consumerCountStr == "" {
		consumerCountStr = "1"
	}
	consumerCount, err := strconv.Atoi(consumerCountStr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse CONSUMER_COUNT: %w", err)
	}

	f.taskReceiverRatings, err = middlewareConnection.ConsumeFrom(f.inputToSave.Name(), groupQueueName, "0", prefetch, uint(1))
	if err != nil {
		return nil, fmt.Errorf("failed to create read queue clean_ratings for task %s", f.Name())
	}
	f.log.Infof("Created read queue exchange %s with groupName %s", f.Name(), groupQueueName)

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
	inputChannelRatings := make(chan middleware.Envelope[*model.Row])

	go func() {
		for {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			envelope, err := f.taskReceiverRatings.Next(ctx)
			if err != nil {
				if err.Error() == "read channel was closed" || err.Error() == "close channel was closed" || err.Error() == "read channel is not initialized" {
					f.log.Infof("Channel for ratings closed from task: %v", f.Name())
					break
				}
				continue
			}
			inputChannelRatings <- envelope
		}
		close(inputChannelRatings)
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
				f.log.Errorf("Error reading from middleware: %v", err)
				continue
			}
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
	pendings, ok := f.pendingMovies[clientID]
	if !ok {
		f.log.Infof("No pending movies for client %s", clientID)
		return nil
	}

	var err error
	for _, row := range pendings {
		err = f.processMovieAndSendRatings(row)
		if err != nil {
			return err
		}
	}
	delete(f.pendingMovies, clientID)
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

func reloadPendingMoviesFromDisk(id string, log *logger.ConsoleLogger) (map[string]map[string]*model.Row, map[string]map[string]*model.Row) {
	rootDir := fmt.Sprintf("joiner_ratings/joiner%s", id)
	log.Infof("Reloading pending movies from: %s", rootDir)

	pendingMovies := make(map[string]map[string]*model.Row)
	processedMovies := make(map[string]map[string]*model.Row)
	clientDirs, err := os.ReadDir(rootDir)
	if err != nil {
		return pendingMovies, processedMovies
	}

	for _, clientDir := range clientDirs {
		if !clientDir.IsDir() {
			continue
		}
		clientId := clientDir.Name()
		clientPath := fmt.Sprintf("%s/%s", rootDir, clientId)

		files, err := os.ReadDir(clientPath)
		if err != nil {
			continue
		}

		movies := make(map[string]*model.Row)
		processed := make(map[string]*model.Row)

		for _, file := range files {
			if strings.HasPrefix(file.Name(), "processed_movies_") {
				fileName := fmt.Sprintf("%s/%s", clientPath, file.Name())
				if err := readCSVToMap(fileName, processed); err != nil {
					continue
				}
			}

			if strings.HasPrefix(file.Name(), "movies_") {
				fileName := fmt.Sprintf("%s/%s", clientPath, file.Name())
				if err := readCSVToMap(fileName, movies); err != nil {
					continue
				}
			}
		}

		processedMovies[clientId] = processed

		for movieID, row := range movies {
			if _, ok := processed[movieID]; !ok {
				if _, exists := pendingMovies[clientId]; !exists {
					pendingMovies[clientId] = make(map[string]*model.Row)
				}
				pendingMovies[clientId][movieID] = row
			}
		}
	}
	return pendingMovies, processedMovies
}

func readCSVToMap(fileName string, movies map[string]*model.Row) error {
	file, err := os.Open(fileName)
	if err != nil {
		return fmt.Errorf("failed to open file %s: %w", fileName, err)
	}
	defer file.Close()
	reader := csv.NewReader(file)
	_, err = reader.Read()
	if err != nil {
		return fmt.Errorf("failed to read header from file %s: %w", fileName, err)
	}
	for {
		data, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil || len(data) < 2 {
			return fmt.Errorf("invalid row in file %s: %v", fileName, err)
		}
		movieID := data[0]
		title := data[1]
		movies[movieID] = &model.Row{Strings: map[string]string{"movieID": movieID, "title": title}}
	}
	return nil

}

func (f *JoinerRatings) getRatingFilename(clientId string, movieID string) (string, error) {
	return f.getFileName(clientId, movieID, "ratings")
}

func (f *JoinerRatings) getFileName(clientId string, movieID string, fileType string) (string, error) {
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

	return fmt.Sprintf("%s/%s_%s.csv", dirPath, fileType, lastDigit), nil
}

func (f *JoinerRatings) getMoviesFilename(clientId string, movieID string) (string, error) {
	return f.getFileName(clientId, movieID, "movies")
}

func (f *JoinerRatings) getProcessedMoviesFilename(clientId string, movieID string) (string, error) {
	return f.getFileName(clientId, movieID, "processed_movies")
}

func (f *JoinerRatings) SavePendingMovie(clientID string, movieID string, title string) error {
	fileName, err := f.getMoviesFilename(clientID, movieID)
	if err != nil {
		f.log.Errorf("Failed to get filename: %s", err)
		return err
	}

	return f.SaveMovie(clientID, movieID, fileName, title)
}

func (f *JoinerRatings) SaveProcessedMovie(clientID string, movieID string, title string) error {
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

	err = f.SaveMovie(clientID, movieID, fileName, title)
	if err != nil {
		return err
	}
	f.processedMovies[clientID][movieID] = &model.Row{Strings: map[string]string{"movieID": movieID, "title": title}}
	return nil
}

func (f *JoinerRatings) SaveMovie(clientID string, movieID string, fileName string, title string) error {

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
		if err := writer.Write([]string{"movieID", "title"}); err != nil {
			f.log.Errorf("Failed to write CSV header: %v", err)
			return err
		}
	}

	err = writer.Write([]string{movieID, title})
	if err != nil {
		f.log.Errorf("Failed to write CSV row: %v", err)
		return err
	}
	writer.Flush()
	if err := writer.Error(); err != nil {
		f.log.Errorf("Flush error writing CSV: %v", err)
		return err
	}
	return nil
}
