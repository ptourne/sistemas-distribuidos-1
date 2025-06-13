package joiner

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/ptourne/sistemas-distribuidos-1/common/logger"
	"github.com/ptourne/sistemas-distribuidos-1/common/model"
	"github.com/ptourne/sistemas-distribuidos-1/joiners/joiner_credits_worker/credits"
	"github.com/ptourne/sistemas-distribuidos-1/joiners_ratings_workers/joiner_ratings_worker/ratings"
	"github.com/ptourne/sistemas-distribuidos-1/middleware/codec"
	"github.com/ptourne/sistemas-distribuidos-1/middleware/middleware"
	"github.com/ptourne/sistemas-distribuidos-1/worker/task"
)

const MIDDLEWARE = "rabbitmq"

type Worker struct {
	Tasks                 task.JoinerTask[*model.Row, *model.Row]
	ClientsFinishedMovies map[uint64]middleware.Envelope[*model.Row]
	ClientsMoviesEOFs     map[uint64]middleware.Envelope[*model.Row]
	ClientsFinishedInput  map[uint64]middleware.Envelope[*model.Row]
	ClientsFinished       map[uint64]bool
}

type TType int

const (
	Bin TType = iota
	Row
)

func (w *Worker) Run(middlewareConnection middleware.Connection[*model.Row]) {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	log := w.Tasks.Logger()
	log.Infof("Connected to middleware: %s", MIDDLEWARE)

	inputChannels, err := w.Tasks.Connect(middlewareConnection, middlewareConnection)
	if err != nil {
		log.Fatalf("Failed to create channel for task %s: %s", w.Tasks.Name(), err)
	}
	log.Infof("Connected to task %s", w.Tasks.Name())

	closed := 0

	var envelope middleware.Envelope[*model.Row]
	var ok bool
	currentTask := w.Tasks
	id := currentTask.Id()

	moviesEOFsChan, receiverMoviesEOFs, senderMoviesEOFs := w.setupMoviesEOFHandling(middlewareConnection, log, id)
	go receiveMoviesEOFFromPeers(receiverMoviesEOFs, moviesEOFsChan, log)

	for {
		select {
		case <-ctx.Done():
			w.shutdown(middlewareConnection, receiverMoviesEOFs, senderMoviesEOFs, moviesEOFsChan, log)
			return
		case envelope, ok = <-inputChannels[0]:
			if envelope != nil && envelope.Type() == middleware.EOF {
				log.Infof("Movies: EOF message received from %d", envelope.Cid())
				_, exists := w.ClientsFinishedMovies[envelope.Cid()]
				if exists {
					log.Errorf("Client %d finished but already registered", envelope.Cid())
					err = envelope.Ack(false)
					if err != nil {
						log.Warnf("Failed to ack EOF message for movies: %v", err)
					}
					continue
				}
				w.ClientsFinishedMovies[envelope.Cid()] = envelope
				senderMoviesEOFs.SendEOF(envelope.Cid())

			} else if envelope != nil && envelope.Type() == middleware.Prune {
				log.Infof("Movies: Received prune message for client %d", envelope.Cid())
				err = envelope.Ack(false)
				if err != nil {
					log.Warnf("Failed to ack prune message for movies: %v", err)
				}
				continue
			} else if !ok {
				log.Infof("Channel closed 0, exiting...")
				closed++
				inputChannels[0] = nil
			}
		case envelope, ok = <-inputChannels[1]:
			if envelope != nil && envelope.Type() == middleware.EOF {
				log.Infof("Credits/Ratings: EOF message received from %d", envelope.Cid())
				_, exists := w.ClientsFinishedInput[envelope.Cid()]
				if exists {
					log.Errorf("Client %d finished but already registered", envelope.Cid())
					err = envelope.Ack(false)
					if err != nil {
						log.Warnf("Failed to ack EOF message for credits/ratings: %v", err)
					}
					continue
				}
				w.ClientsFinishedInput[envelope.Cid()] = envelope
				currentTask.ProcessPendingMovies(envelope.Cid())
			} else if envelope != nil && envelope.Type() == middleware.Prune {
				log.Infof("Credits/Ratings: Received prune message for client %d", envelope.Cid())
				err = envelope.Ack(false)
				if err != nil {
					log.Warnf("Failed to ack prune message for credits/ratings: %v", err)
				}
				continue

			} else if !ok {
				log.Infof("Channel closed 1, exiting...")
				inputChannels[1] = nil
				closed++
			}
		case envelope, ok = <-moviesEOFsChan:
			if !ok {
				log.Infof("Channel moviesEOFsChan closed")
				continue
			}
			if ok && envelope != nil {
				mustACK := true
				if envelope.Type() == middleware.EOF {
					_, exists := w.ClientsMoviesEOFs[envelope.Cid()]
					_, finished := w.ClientsFinished[envelope.Cid()]
					if !exists && !finished {
						log.Infof("MoviesEOFs: EOF message received for %d", envelope.Cid())
						w.ClientsMoviesEOFs[envelope.Cid()] = envelope
						mustACK = false
					}
				}
				if mustACK {
					log.Infof("MoviesEOFs: Acking message type %s for %d", envelope.Type(), envelope.Cid())
					err = envelope.Ack(false)
					if err != nil {
						log.Warnf("Failed to ack EOF message for moviesEOFs: %v", err)
					}
					continue
				}

			}
		}

		if closed == 2 {
			break
		}
		if !ok && (envelope == nil || envelope.Type() == middleware.Normal) {
			continue
		}

		if envelope != nil && envelope.Type() == middleware.EOF {
			w.processEOF(envelope, id, log, currentTask)
			continue

		}

		if envelope == nil || envelope.Msg() == nil {
			log.Warnf("Received nil message: %+v", envelope)
			continue
		}

		//log.Infof("Received message: %v from %s", envelope.Msg(), envelope.Cid())

		result := currentTask.ProcessAndSend(envelope)
		if result != nil {
			log.Errorf("Failed to process row: %v by task: %v", envelope.Msg(), currentTask.Name())
			continue
		}
		log.Debugf("TO ACK msg %v worker", envelope.Msg())
		err = envelope.Ack(false)
		if err != nil {
			log.Warnf("Failed to ack row message: %v", err)
		}
	}
	currentTask.Finish()
}

func (w *Worker) setupMoviesEOFHandling(conn middleware.Connection[*model.Row], log *logger.ConsoleLogger, id string) (chan middleware.Envelope[*model.Row], middleware.Receiver[*model.Row], middleware.Sender[*model.Row]) {
	groupQueueName := w.Tasks.NameWithId()

	prefetch := 100

	peerCount := getWorkerCount(log)

	joinerType := "credits"
	if strings.Contains(w.Tasks.Name(), "ratings") {
		joinerType = "ratings"
	}

	exchangeName := fmt.Sprintf("moviesEOFs_%s", joinerType)

	receiver, err := conn.ConsumeFrom(exchangeName, groupQueueName, "0", prefetch, uint(1))
	unwrap(err, "Failed to consume moviesEOFs", log)

	moviesEOFsChan := make(chan middleware.Envelope[*model.Row])

	var sender middleware.Sender[*model.Row]

	if id == "0" {
		subscribers := generateSubscribers(peerCount, joinerType)
		sender, err = conn.WriteTo(exchangeName, subscribers, id, uint(1))
		unwrap(err, "Failed to create moviesEOFs sender", log)
	}

	return moviesEOFsChan, receiver, sender
}

func getWorkerCount(log *logger.ConsoleLogger) int {
	countStr := os.Getenv("WORKER_COUNT")
	if countStr == "" {
		log.Errorf("WORKER_COUNT not set, defaulting to 1")
		countStr = "1"
	}
	peers, err := strconv.Atoi(countStr)
	unwrap(err, "Invalid WORKER_COUNT", log)
	return peers
}

func generateSubscribers(count int, joinerType string) []string {
	prefix := "joiner"
	subs := []string{}
	for i := range count {
		subs = append(subs, fmt.Sprintf("%s_%d_%s", prefix, i, joinerType))
	}
	return subs
}

func (w *Worker) shutdown(middlewareConnection middleware.Connection[*model.Row], receiverMoviesEOFs middleware.Receiver[*model.Row], senderMoviesEOFs middleware.Sender[*model.Row], moviesEOFsChan chan middleware.Envelope[*model.Row], log *logger.ConsoleLogger) {
	log.Infof("Received termination signal, shutting down gracefully...")
	var err error
	currentTask := w.Tasks
	currentTask.Finish()
	if receiverMoviesEOFs != nil {
		err = receiverMoviesEOFs.Close()
		if err != nil {
			log.Errorf("Failed to close receiver channel: %v", err)
		}
	}
	if senderMoviesEOFs != nil {
		err = senderMoviesEOFs.Close()
		if err != nil {
			log.Errorf("Failed to close receiver channel: %v", err)
		}
	}
	close(moviesEOFsChan)
	err = middlewareConnection.Close()
	if err != nil {
		log.Errorf("Failed to close middleware connection: %v", err)
	}
	log.Infof("Graceful shutdown complete")
}

func (w *Worker) processEOF(envelope middleware.Envelope[*model.Row], id string, log *logger.ConsoleLogger, currentTask task.JoinerTask[*model.Row, *model.Row]) {
	var err error
	eofMovies, existsMovies := w.ClientsFinishedMovies[envelope.Cid()]
	eofMovies2, existsMovies2 := w.ClientsMoviesEOFs[envelope.Cid()]
	eofInput, existsInput := w.ClientsFinishedInput[envelope.Cid()]
	if (existsMovies || existsMovies2) && existsInput {
		log.Infof("Client %d finished", envelope.Cid())
		delete(w.ClientsFinishedMovies, envelope.Cid())
		delete(w.ClientsFinishedInput, envelope.Cid())
		if id == "0" {
			currentTask.FinishProcessingClient(envelope.Cid(), true)
		} else {
			currentTask.FinishProcessingClient(envelope.Cid(), false)
		}
		log.Infof("Finished processing client %d", envelope.Cid())
		if existsMovies {
			err = eofMovies.Ack(false)
			if err != nil {
				log.Warnf("Failed to ack EOF message for movies: %v", err)
			}
		}
		if existsMovies2 {
			err = eofMovies2.Ack(false)
			if err != nil {
				log.Warnf("Failed to ack EOF message for moviesEOFs: %v", err)
			}
		}
		err = eofInput.Ack(false)
		if err != nil {
			log.Warnf("Failed to ack EOF message for credits/ratings: %v", err)
		}
		w.ClientsFinished[envelope.Cid()] = true
	}
}

func receiveMoviesEOFFromPeers(receiverMoviesEOFs middleware.Receiver[*model.Row], moviesEOFsChan chan middleware.Envelope[*model.Row], log *logger.ConsoleLogger) {
	for {
		ctxReceive, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		envelope, err := receiverMoviesEOFs.Next(ctxReceive)
		if err != nil {
			if err.Error() == "read channel was closed" || err.Error() == "close channel was closed" {
				log.Infof("Channel for moviesEOF closed from task Joniner")
				break
			}
			continue
		}
		moviesEOFsChan <- envelope
	}

	close(moviesEOFsChan)
}

func unwrap(err error, msg string, log *logger.ConsoleLogger) {
	if err != nil {
		if strings.Contains(err.Error(), "channel/connection is not open") {
			log.Warnf("%s: %s", msg, err)
		} else {
			log.Fatalf("%s: %s", msg, err)
			panic(err)
		}
	}
}

type SourceTask[O codec.Serializable[O]] struct {
	name string
}

func NewSourceTask[O codec.Serializable[O]](name string) task.Task[*model.Row, O] {
	return &SourceTask[O]{name}
}

func (t *SourceTask[O]) ProcessAndSend(r middleware.Envelope[*model.Row]) error {
	return nil
}

func (t *SourceTask[O]) Name() string {
	return t.name
}

func (t *SourceTask[O]) Input() string {
	return ""
}

func (t *SourceTask[O]) Finish() error {
	return nil
}

func (t *SourceTask[O]) Connect(_ middleware.Connection[*model.Row], _ middleware.Connection[O]) ([]chan middleware.Envelope[*model.Row], error) {
	return nil, nil
}

func NewCreditsWorker(subscribers []string, id string, workerLogger *logger.ConsoleLogger) Worker {
	movies := NewSourceTask[*model.Row]("filter_release_date_ge_2000_and_include_ar")
	clean_credits := NewSourceTask[*model.Row]("clean_credits")
	joiner_credits := credits.NewJoinerCredits(movies, clean_credits, subscribers, id, workerLogger)

	return Worker{
		Tasks:                 joiner_credits,
		ClientsFinishedMovies: make(map[uint64]middleware.Envelope[*model.Row]),
		ClientsMoviesEOFs:     make(map[uint64]middleware.Envelope[*model.Row]),
		ClientsFinishedInput:  make(map[uint64]middleware.Envelope[*model.Row]),
		ClientsFinished:       make(map[uint64]bool),
	}
}

func NewRatingsWorker(subscribers []string, id string, workerLogger *logger.ConsoleLogger) Worker {
	movies_metadata := NewSourceTask[*model.Row]("filter_release_date_ge_2000_and_include_ar")
	filter_avg_rating := NewSourceTask[*model.Row]("filter_avg_rating")

	joiner_ratings := ratings.NewJoinerRatings(movies_metadata, filter_avg_rating, subscribers, id, workerLogger)

	return Worker{
		Tasks:                 joiner_ratings,
		ClientsFinishedMovies: make(map[uint64]middleware.Envelope[*model.Row]),
		ClientsMoviesEOFs:     make(map[uint64]middleware.Envelope[*model.Row]),
		ClientsFinishedInput:  make(map[uint64]middleware.Envelope[*model.Row]),
		ClientsFinished:       make(map[uint64]bool),
	}
}
