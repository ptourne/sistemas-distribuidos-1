package joiner

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/ptourne/sistemas-distribuidos-1/common"
	"github.com/ptourne/sistemas-distribuidos-1/middleware"
	"github.com/ptourne/sistemas-distribuidos-1/worker/task"
)

type JoinerRatings struct {
	inputToProcess      task.Task
	inputToSave         task.Task
	taskReceiverRatings middleware.Receiver[common.Row]
	taskReceiverMovies  middleware.Receiver[common.Row]
	taskSender          middleware.Sender[common.Row]
	ratingsProcessed    int
	doneRatings         atomic.Bool
	pendingMovies       map[string]common.Row
	pendingMoviesMu     sync.Mutex
	subscribers         []string
	writers             []*csv.Writer
	toWrite             [][][]string
	toWriteLen          []int
}

const TO_WRITE_THRESHOLD = 1000

func NewJoinerRatings(inputToProcess task.Task, inputToSave task.Task, subscribers []string) task.Task {
	toWrite := make([][][]string, 10)
	for i := range toWrite {
		toWrite[i] = make([][]string, TO_WRITE_THRESHOLD+1)
	}
	toWriteLen := make([]int, 10)
	joiner := JoinerRatings{inputToProcess, inputToSave, nil, nil, nil, 0, atomic.Bool{}, make(map[string]common.Row), sync.Mutex{}, subscribers, []*csv.Writer{}, toWrite, toWriteLen}
	joiner.doneRatings.Store(false)
	return &joiner
}

func (f *JoinerRatings) Input() string {
	return f.inputToProcess.Name()
}

func (f *JoinerRatings) Name() string {
	return "joiner_ratings"
}

func (f *JoinerRatings) ProcessAndSend(row common.Row) error {
	var err error
	movieID := row.Strings["movieID"]
	if title, ok := row.Strings["title"]; ok && title != "" {
		//log.Infof("Processing movie: %s", movieID)
		if !f.doneRatings.Load() {
			log.Infof("Adding movie to pending: %s", movieID)
			f.pendingMoviesMu.Lock()
			f.pendingMovies[movieID] = row
			f.pendingMoviesMu.Unlock()

			return nil
		}
		err = f.processMovieAndSendRatings(row)
	} else if _, ok := row.Floats["rating"]; ok { // cambiar por avg_rating
		err = f.processRating(row)
	} else {
		log.Warnf("Received row with no recognizable ID: %+v", row)
	}

	return err
}

func (f *JoinerRatings) processMovieAndSendRatings(row common.Row) error {

	output, err := f.processMovie(row)
	return f.sendRating(output, err)
}

func (f *JoinerRatings) sendRating(output *common.Row, err error) error {
	if err != nil {
		//log.Errorf("Failed to process movie: %v", err)
		return err
	}
	if output == nil {
		return nil
	}
	err = f.taskSender.Send(output)
	if err != nil {
		log.Errorf("Failed to send rating data: %v", err)
		return err
	}

	return nil
}

func (f *JoinerRatings) processRating(row common.Row) error {
	f.ratingsProcessed++
	movieID := row.Strings["movieID"]
	rating := row.Floats["rating"]
	log.Infof("Processing rating %v", f.ratingsProcessed)

	lastDigitStr := string(movieID[len(movieID)-1])

	lastDigit, err := strconv.ParseInt(lastDigitStr, 10, 8)
	if err != nil {
		log.Errorf("Failed to parse last digit: %v", err)
		return err
	}
	ratingString := strconv.FormatFloat(rating, 'f', 6, 64)
	log.Debugf("toWriteLen[%d]: %d", lastDigit, f.toWriteLen[lastDigit])
	f.toWrite[lastDigit][f.toWriteLen[lastDigit]] = []string{movieID, ratingString}
	f.toWriteLen[lastDigit]++
	if f.toWriteLen[lastDigit] < TO_WRITE_THRESHOLD-1 {
		log.Debugf("skipping write")
		return nil
	}
	log.Debugf("writting %d rows", f.toWriteLen[lastDigit])
	writer := f.writers[lastDigit]
	// defer writer.Flush()

	err = writer.WriteAll(f.toWrite[lastDigit][:f.toWriteLen[lastDigit]])
	if err != nil {
		log.Errorf("Failed to write CSV row: %v", err)
		return err
	}
	f.toWrite[lastDigit] = make([][]string, TO_WRITE_THRESHOLD+1)
	f.toWriteLen[lastDigit] = 0

	// ToDo: descomentar cuando este el reducer testeado
	// f.pendingMoviesMu.Lock()
	// movie, found := f.pendingMovies[movieID]
	// if found {
	// 	delete(f.pendingMovies, movieID)
	// }
	// f.pendingMoviesMu.Unlock()

	// if found {
	// 	log.Infof("Processing pending movie: %s", movieID)
	// 	roeRes := &common.Row{
	// 		Strings: map[string]string{
	// 			"movieID": movieID,
	// 			"title":   movie.Strings["title"],
	// 		},
	// 		Floats: map[string]float64{

	// 			"avg_rating": rating,
	// 		},
	// 	}
	// 	err = f.sendRating(roeRes, nil)
	// 	if err != nil {
	// 		log.Errorf("Failed to process pending movie: %v", err)
	// 	}
	// }

	// if f.ratingsProcessed == 10000 { //TODO: sacar cuando se mergee con los cambios del reducer
	// 	f.notifyRatingsDone()
	// }

	return nil
}

func (f *JoinerRatings) processMovie(row common.Row) (*common.Row, error) {
	movieID := row.Strings["movieID"]
	title := row.Strings["title"]
	//log.Infof("Processing movie: %s", movieID)
	lastDigit := string(movieID[len(movieID)-1])
	dirPath := fmt.Sprintf("joiner_ratings/joiner%s", WORKER_ID)

	fileName := fmt.Sprintf("%s/ratings_%s.csv", dirPath, lastDigit)
	file, err := os.Open(fileName)
	if err != nil {
		log.Errorf("Failed to open file: %s", fileName)
		return nil, err

	}
	defer file.Close()

	reader := csv.NewReader(file)
	_, err = reader.Read()
	if err != nil {
		log.Errorf("Failed to read header: %s", err)
		return nil, err
	}

	var ratings []float64 // ToDo: cambiar por float64 cuando este el reducer testeado

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
			ratings = append(ratings, rating)
		}
	}

	if len(ratings) == 0 {
		log.Infof("No ratings found for movie %s", movieID)
		return nil, nil
	}

	sum := 0.0
	for _, r := range ratings {
		sum += r
	}

	var avg float64
	if len(ratings) > 0 {
		avg = sum / float64(len(ratings))
	} else {
		avg = 0
	}
	//log.Infof("Average rating for movie %s: %f", movieID, avg)

	rowRes := &common.Row{
		Strings: map[string]string{
			"movieID": movieID,
			"title":   title,
		},
		Floats: map[string]float64{
			"avg_rating": avg,
		},
	}

	return rowRes, nil

}

func (f *JoinerRatings) Connect(middlewareConnection middleware.MiddlewareCola[common.Row]) ([]chan middleware.Envelope[common.Row], error) {
	var err error
	groupQueueName := fmt.Sprintf("joiner_%s_ratings", WORKER_ID)
	f.taskReceiverRatings, err = middlewareConnection.ConsumeFrom(f.inputToSave.Name(), groupQueueName)
	if err != nil {
		return nil, fmt.Errorf("failed to create read queue clean_ratings for task %s", f.Name())
	}

	f.taskReceiverMovies, err = middlewareConnection.ConsumeFrom(f.Input(), f.Name())
	if err != nil {
		return nil, fmt.Errorf("failed to create read queue %s for task %s", f.Input(), f.Name())
	}

	f.taskSender, err = middlewareConnection.WriteTo(f.Name(), f.subscribers)
	if err != nil {
		return nil, fmt.Errorf("failed to create write queue for task %s", f.Name())
	}

	//lint:ignore S1019 Ignorar reflect.Select en este archivo
	inputChannelMovies := make(chan middleware.Envelope[common.Row], 0)
	inputChannelRatings := make(chan middleware.Envelope[common.Row])

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
		f.notifyRatingsDone()
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

	f.writers = make([]*csv.Writer, 10)
	for lastDigit := range f.writers {

		if err := os.MkdirAll("joiner_ratings", os.ModePerm); err != nil {
			log.Errorf("Failed to create directory: %s", "joiner_ratings")
			return nil, err
		}
		dirPath := fmt.Sprintf("joiner_ratings/joiner%s", WORKER_ID)

		if err := os.MkdirAll(dirPath, os.ModePerm); err != nil {
			log.Errorf("Failed to create directory: %s", dirPath)
			return nil, err
		}

		fileName := fmt.Sprintf("%s/ratings_%d.csv", dirPath, lastDigit)

		file, err := os.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			log.Errorf("Failed to open file: %s", fileName)
			return nil, err
		}
		writer := csv.NewWriter(file)

		if err := writer.Write([]string{"movieID", "rating"}); err != nil {
			log.Errorf("Failed to write CSV header: %v", err)
			return nil, err

		}
		f.writers[lastDigit] = writer
	}

	inputChannel := make([]chan middleware.Envelope[common.Row], 2)
	inputChannel[0] = inputChannelMovies
	inputChannel[1] = inputChannelRatings
	return inputChannel, nil
}

func (f *JoinerRatings) Finish() error {
	if !f.doneRatings.Load() {
		log.Infof("Cannot finish: still processing ratings")
		return fmt.Errorf("cannot finish: still receiving ratings")
	}
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

func (f *JoinerRatings) notifyRatingsDone() {
	log.Infof("Finished writing ratings to file by worker %v, have processed %d ratings", WORKER_ID, f.ratingsProcessed)
	f.doneRatings.Store(true)
	f.pendingMoviesMu.Lock()
	pendings := f.pendingMovies
	f.pendingMovies = nil
	f.pendingMoviesMu.Unlock()

	for _, row := range pendings {
		//log.Infof("Process pending movie: %s", row.Strings["movieID"])
		f.processMovieAndSendRatings(row)
	}
}
