package joiner

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strconv"
	"sync"

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
}

func NewJoinerRatings(inputToProcess task.Task[*model.Row, *model.Row], inputToSave task.Task[*model.Row, *model.Row], subscribers []string) task.JoinerTask[*model.Row, *model.Row] {
	joiner := JoinerRatings{inputToProcess, inputToSave, nil, nil, nil, 0, make(map[string]map[string]*model.Row), sync.Mutex{}, subscribers, make(map[string]bool)}
	return &joiner
}

func (f *JoinerRatings) Input() string {
	return f.inputToProcess.Name()
}

func (f *JoinerRatings) Name() string {
	return "joiner_ratings"
}

func (f *JoinerRatings) ProcessAndSend(row *model.Row) error {
	var err error
	//movieID := row.Strings["movieID"]
	if title, ok := row.Strings["title"]; ok && title != "" {
		//log.Infof("Processing movie: %s", movieID)
		// if !f.doneRatings.Load() {
		// 	log.Infof("Adding movie to pending: %s", movieID)
		// 	f.pendingMoviesMu.Lock()
		// 	f.pendingMovies[movieID] = row
		// 	f.pendingMoviesMu.Unlock()
		// 	return nil
		// }
		err = f.processMovieAndSendRatings(row)
	} else if _, ok := row.Floats["avg_rating"]; ok {
		err = f.processRating(row)
	} else {
		log.Warnf("Received row with no recognizable ID: %+v", row)
	}

	return err
}

func (f *JoinerRatings) processMovieAndSendRatings(row *model.Row) error {

	output, err := f.processMovie(row)
	if err != nil {
		//log.Errorf("Failed to process movie: %v", err)
		if err.Error() == "no rating found" {
			clientID := row.Strings["cid"]
			_, hasFinished := f.finishedRatings[clientID]
			if hasFinished {
				log.Infof("No rating found for movie %s", row.Strings["movieID"])
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
				log.Infof("Adding movie %s to pending movies", row.Strings["movieID"])
			} else {
				delete(f.pendingMovies[clientID], row.Strings["movieID"])
				log.Infof("No rating found for movie %s", row.Strings["movieID"])
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

	log.Infof("Sending rating data: %v", output)
	err := f.taskSender.Send(output, cid)
	if err != nil {
		log.Errorf("Failed to send rating data: %v", err)
		return err
	}

	return nil
}

func (f *JoinerRatings) processRating(row *model.Row) error {
	f.ratingsProcessed++
	movieID := row.Strings["movieID"]
	avg_rating := row.Floats["avg_rating"]
	log.Infof("Processing rating %v : %v", f.ratingsProcessed, row)

	lastDigit := string(movieID[len(movieID)-1])

	clientId := row.Strings["cid"]
	if err := os.MkdirAll(clientId, os.ModePerm); err != nil {
		log.Errorf("Failed to create directory: %s", clientId)
		return err
	}
	dirPath := fmt.Sprintf("%s/joiner_ratings", clientId)
	if err := os.MkdirAll(dirPath, os.ModePerm); err != nil {
		log.Errorf("Failed to create directory: %s", "joiner_ratings")
		return err
	}
	dirPath = fmt.Sprintf("%s/joiner%s", dirPath, WORKER_ID)

	if err := os.MkdirAll(dirPath, os.ModePerm); err != nil {
		log.Errorf("Failed to create directory: %s", dirPath)
		return err
	}

	fileName := fmt.Sprintf("%s/ratings_%s.csv", dirPath, lastDigit)

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
		if err := writer.Write([]string{"movieID", "rating"}); err != nil {
			log.Errorf("Failed to write CSV header: %v", err)
			return err
		}
	}

	ratingString := fmt.Sprintf("%f", avg_rating)

	err = writer.Write([]string{movieID, ratingString})
	if err != nil {
		log.Errorf("Failed to write CSV row: %v", err)
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
			//log.Infof("Found pending movie: %s in pending movies %v", movieID, f.pendingMovies)
			delete(f.pendingMovies[clientId], movieID)
		}
	}
	f.pendingMoviesMu.Unlock()

	if found {
		log.Infof("Processing pending movie: %s", movieID)
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
			log.Errorf("Failed to process pending movie: %v", err)
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
	log.Infof("Processing movie: %s", movieID)
	lastDigit := string(movieID[len(movieID)-1])
	clientId := row.Strings["cid"]

	dirPath := fmt.Sprintf("%s/joiner_ratings/joiner%s", clientId, WORKER_ID)

	fileName := fmt.Sprintf("%s/ratings_%s.csv", dirPath, lastDigit)
	file, err := os.Open(fileName)
	if err != nil {
		log.Errorf("Failed to open file: %s", fileName)
		return nil, fmt.Errorf("no rating found")

	}
	defer file.Close()

	reader := csv.NewReader(file)
	_, err = reader.Read()
	if err != nil {
		log.Errorf("Failed to read header: %s", err)
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
			log.Errorf("Invalid ratings row: %v", err)
			continue
		}

		if data[0] == movieID {
			ratingString := data[1]
			rating, err := strconv.ParseFloat(ratingString, 64)
			if err != nil {
				log.Errorf("Failed to parse rating: %v; rating = %v", err, ratingString)
				continue

			}
			//log.Infof("Adding rating for movie %s, %f", movieID, rating)
			avg_rating = rating
			found = true
			break
		}
	}

	if !found {
		//log.Infof("No ratings found for movie %s", movieID)
		return nil, fmt.Errorf("no rating found")
	}
	//log.Infof("Average rating for movie %s: %f", movieID, avg)

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
	// var WORKER_COUNT_STR = os.Getenv("WORKER_COUNT")
	// WORKER_COUNT, err := strconv.Atoi(WORKER_COUNT_STR)
	// if err != nil {
	// 	return nil, fmt.Errorf("failed to parse WORKER_COUNT: %w", err)
	// }
	// peers := WORKER_COUNT - 1
	groupQueueName := fmt.Sprintf("joiner_%s_ratings", WORKER_ID)
	f.taskReceiverRatings, err = middlewareConnection.ConsumeFrom(f.inputToSave.Name(), groupQueueName, 0, 2) // ToDo: usar los valores reales de 'peers' y 'prefetch'
	if err != nil {
		return nil, fmt.Errorf("failed to create read queue clean_ratings for task %s", f.Name())
	}
	log.Infof("Created read queue exchange %s with groupName %s", f.Name(), groupQueueName)

	f.taskReceiverMovies, err = middlewareConnection.ConsumeFrom(f.Input(), f.Name(), 0, 2) // ToDo: usar los valores reales de 'peers' y 'prefetch'
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
			envelope, ok, err := f.taskReceiverRatings.Next(nil)
			if err != nil {
				if err.Error() == "read channel was closed" || err.Error() == "close channel was closed" {
					log.Infof("Channel for ratings closed from task: %v", f.Name())
					break
				}
				log.Errorf("Error reading from middleware (joiner_ratings): %v", err)
				continue
			}
			if !ok {
				log.Infof("Channel closed (ratings): %v", f.Name())
				break
			}
			inputChannelRatings <- envelope
		}
		//f.notifyRatingsDone()
		close(inputChannelRatings)
	}()

	go func() {
		for {
			envelope, ok, err := f.taskReceiverMovies.Next(nil)
			if err != nil {
				if err.Error() == "read channel was closed" || err.Error() == "close channel was closed" {
					log.Infof("Channel for movies closed from task: %v", f.Name())
					break
				}
				log.Errorf("Error reading from middleware: %v", err)
				continue
			}
			if !ok {
				log.Infof("Channel closed (movies): %v", f.Name())
				break
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
	// if !f.doneRatings.Load() {
	// 	log.Infof("Cannot finish: still processing ratings")
	// 	return fmt.Errorf("cannot finish: still receiving ratings")
	// }
	if err := f.taskReceiverMovies.Close(); err != nil {
		return fmt.Errorf("failed to close task receiver (movies): %w", err)
	}
	if err := f.taskReceiverRatings.Close(); err != nil {
		return fmt.Errorf("failed to close task receiver (ratings): %w", err)
	}
	if err := f.taskSender.Close(); err != nil {
		return fmt.Errorf("failed to close task sender: %w", err)
	}
	log.Infof("Closed task %s", f.Name())
	return nil
}

func (f *JoinerRatings) ProcessPendingMovies(clientID string) error {
	log.Infof("Processing pending movies for client %s", clientID)
	f.pendingMoviesMu.Lock()
	pendings, ok := f.pendingMovies[clientID]
	if !ok {
		log.Infof("No pending movies for client %s", clientID)
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

func (f *JoinerRatings) FinishProcessingClient(clientID string) error {
	f.ProcessPendingMovies(clientID)
	dirPath := fmt.Sprintf("%s/joiner_ratings/joiner%s", clientID, WORKER_ID)
	err := os.RemoveAll(dirPath)
	if err != nil {
		return err
	} else {
		log.Infof("Removed directory: %s", dirPath)
	}
	return nil
}
