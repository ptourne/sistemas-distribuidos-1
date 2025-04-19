package joiner

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/ptourne/sistemas-distribuidos-1/common"
	"github.com/ptourne/sistemas-distribuidos-1/middleware"
	"github.com/ptourne/sistemas-distribuidos-1/worker/task"
)

type JoinerCredits struct {
	inputToProcess      task.Task
	inputToSave         task.Task
	taskReceiverCredits middleware.Receiver[common.Row]
	taskReceiverMovies  middleware.Receiver[common.Row]
	taskSender          middleware.Sender[common.Row]
	creditsProcessed    int
	doneCredits         atomic.Bool
	pendingMovies       []common.Row
	pendingMoviesMu     sync.Mutex
}

func NewJoinerCredits(inputToProcess task.Task, inputToSave task.Task) task.Task {
	joiner := JoinerCredits{inputToProcess, inputToSave, nil, nil, nil, 0, atomic.Bool{}, make([]common.Row, 0), sync.Mutex{}}
	joiner.doneCredits.Store(false)
	return &joiner
}

func (f *JoinerCredits) Input() string {
	return f.inputToProcess.Name()
}

func (f *JoinerCredits) Name() string {
	return "joiner_credits"
}

func (f *JoinerCredits) ProcessAndSend(row common.Row) error {
	var err error
	if movieID, ok := row.Strings["movieID"]; ok && movieID != "" {
		//log.Infof("Processing movie: %s", movieID)
		if !f.doneCredits.Load() {
			//log.Infof("Adding movie to pending: %s", movieID)
			f.pendingMoviesMu.Lock()
			f.pendingMovies = append(f.pendingMovies, row)
			f.pendingMoviesMu.Unlock()

			return nil
		}
		err = f.processAndSendMovie(row)
	} else if _, ok := row.Strings["ID"]; ok {
		err = f.processCredit(row)
	} else {
		log.Warnf("Received row with no recognizable ID: %+v", row)
	}

	return err
}

func (f *JoinerCredits) processAndSendMovie(row common.Row) error {

	output, err := f.processMovie(row)
	if err != nil {
		//log.Errorf("Failed to process movie: %v", err)
		return err
	}
	if output == nil {
		return nil
	}
	for _, r := range output {
		err = f.taskSender.Send(r)
		if err != nil {
			log.Errorf("Failed to send actor data: %v", err)
			return err
		}
	}
	return nil
}

func (f *JoinerCredits) processCredit(row common.Row) error {

	movieID := row.Strings["ID"]
	//log.Infof("Processing credit for movie: %s", movieID)

	lastDigit := string(movieID[len(movieID)-1])

	if err := os.MkdirAll("joiner_credits", os.ModePerm); err != nil {
		log.Errorf("Failed to create directory: %s", "joiner_credits")
		return err
	}
	dirPath := fmt.Sprintf("joiner_credits/joiner%s", WORKER_ID)

	if err := os.MkdirAll(dirPath, os.ModePerm); err != nil {
		log.Errorf("Failed to create directory: %s", dirPath)
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
		if err := writer.Write([]string{"movieID", "cast"}); err != nil {
			log.Errorf("Failed to write CSV header: %v", err)
			return err
		}
	}

	castString, err := json.Marshal(row.Arrays["cast"])
	//log.Infof("Writing cast for from strings=%v, arrays=%v", row.Strings, row.Arrays)
	if err != nil {
		log.Errorf("Failed to marshal cast: %v", err)
		return err
	}

	err = writer.Write([]string{movieID, string(castString)})
	if err != nil {
		log.Errorf("Failed to write CSV row: %v", err)
		return err
	}

	f.creditsProcessed++
	// if f.creditsProcessed == 1000 { //TODO
	// 	f.notifyCreditsDone()
	// }

	return nil
}

func (f *JoinerCredits) processMovie(row common.Row) ([]*common.Row, error) {
	var flattenCast []*common.Row
	movieID := row.Strings["movieID"]

	lastDigit := string(movieID[len(movieID)-1])
	dirPath := fmt.Sprintf("joiner_credits/joiner%s", WORKER_ID)

	fileName := fmt.Sprintf("%s/credits_%s.csv", dirPath, lastDigit)
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

	var cast string

	for {
		data, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil || len(data) < 2 {
			log.Errorf("Invalid credits row: %v", err)
			continue
		}

		if data[0] == movieID {
			//log.Infof("Found cast for %s", movieID)
			cast = data[1]
			break
		}
	}

	if cast == "" {
		log.Infof("No cast found for %s", movieID)
		return nil, nil
	}

	var castList []string
	if err := json.Unmarshal([]byte(cast), &castList); err != nil {
		log.Errorf("Failed to unmarshal cast: %v; cast = %v", err, cast)
		return nil, err
	}

	for _, actor := range castList {
		actor = strings.Trim(actor, " ")
		if actor != "" {
			flattenCast = append(flattenCast, &common.Row{
				Strings: map[string]string{
					"movieID": movieID,
					"actor":   actor,
				},
			})
		}
	}
	return flattenCast, nil

}

func (f *JoinerCredits) Connect(middlewareConnection middleware.MiddlewareCola[common.Row]) ([]chan middleware.Envelope[common.Row], error) {
	var err error
	groupQueueName := fmt.Sprintf("joiner_%s_credits", WORKER_ID)
	f.taskReceiverCredits, err = middlewareConnection.ConsumeFrom(f.inputToSave.Name(), groupQueueName)
	if err != nil {
		return nil, fmt.Errorf("failed to create read queue clean_credits for task %s", f.Name())
	}

	f.taskReceiverMovies, err = middlewareConnection.ConsumeFrom(f.Input(), f.Name())
	if err != nil {
		return nil, fmt.Errorf("failed to create read queue %s for task %s", f.Input(), f.Name())
	}

	f.taskSender, err = middlewareConnection.WriteTo(f.Name())
	if err != nil {
		return nil, fmt.Errorf("failed to create write queue for task %s", f.Name())
	}

	//lint:ignore S1019 Ignorar reflect.Select en este archivo
	inputChannelMovies := make(chan middleware.Envelope[common.Row], 0)
	inputChannelCredits := make(chan middleware.Envelope[common.Row])

	go func() {
		for {
			envelope, ok, err := f.taskReceiverCredits.Next(nil)
			if err != nil {
				if err.Error() == "read channel was closed" || err.Error() == "close channel was closed" {
					log.Infof("Channel for credits closed from task: %v", f.Name())
					break
				}
				log.Errorf("Error reading from middleware: %v", err)
				continue
			}
			if !ok {
				//log.Infof("Channel closed: %v", f.Name())
				break
			}
			inputChannelCredits <- envelope
		}
		f.notifyCreditsDone()
		close(inputChannelCredits)
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
				//log.Infof("Channel closed: %v", f.Name())
				break
			}
			inputChannelMovies <- envelope
		}
		close(inputChannelMovies)
	}()

	inputChannel := make([]chan middleware.Envelope[common.Row], 2)
	inputChannel[0] = inputChannelMovies
	inputChannel[1] = inputChannelCredits
	return inputChannel, nil
}

func (f *JoinerCredits) Finish() error {
	if !f.doneCredits.Load() {
		log.Infof("Cannot finish: still processing credits")
		return fmt.Errorf("cannot finish: still receiving credits")
	}
	if err := f.taskReceiverMovies.Close(); err != nil {
		return fmt.Errorf("failed to close task receiver (movies): %w", err)
	}
	if err := f.taskReceiverCredits.Close(); err != nil {
		return fmt.Errorf("failed to close task receiver (credits): %w", err)
	}
	if err := f.taskSender.Close(); err != nil {
		return fmt.Errorf("failed to close task sender: %w", err)
	}
	log.Infof("Closed task %s", f.Name())
	return nil
}

func (f *JoinerCredits) notifyCreditsDone() {
	log.Infof("Finished writing credits to file")
	f.doneCredits.Store(true)
	f.pendingMoviesMu.Lock()
	pendings := f.pendingMovies
	f.pendingMovies = nil
	f.pendingMoviesMu.Unlock()

	for _, row := range pendings {
		log.Infof("Process pending movie: %s", row.Strings["movieID"])
		f.processAndSendMovie(row)
	}
}
