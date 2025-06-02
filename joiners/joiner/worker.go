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
	"github.com/ptourne/sistemas-distribuidos-1/middleware/middleware/rabbitmq"
	"github.com/ptourne/sistemas-distribuidos-1/worker/task"
	"github.com/rabbitmq/amqp091-go"
)

const MIDDLEWARE = "rabbitmq"

type Worker struct {
	Tasks task.JoinerTask[*model.Row, *model.Row]
}

type TType int

const (
	Bin TType = iota
	Row
)

func (w *Worker) Run(middlewareConnection middleware.Connection[*model.Row]) {
	log := w.Tasks.Logger()
	log.Infof("Connected to middleware: %s", MIDDLEWARE)

	inputChannels, err := w.Tasks.Connect(middlewareConnection, middlewareConnection)
	if err != nil {
		log.Fatalf("Failed to create channel for task %s: %s", w.Tasks.Name(), err)
	}
	log.Infof("Connected to task %s", w.Tasks.Name())

	closed := 0
	clientsFinishedMovies := make(map[string]middleware.Envelope[*model.Row])
	clientsFinishedInput := make(map[string]middleware.Envelope[*model.Row])
	clientsFinished := make(map[string]bool)
	var envelope middleware.Envelope[*model.Row]
	var envelopeEOF middleware.Envelope[*model.Row]
	var ok bool
	currentTask := w.Tasks
	id := currentTask.Id()
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	groupQueueName := currentTask.NameWithId()

	var WORKER_COUNT_STR = os.Getenv("WORKER_COUNT")
	if WORKER_COUNT_STR == "" {
		log.Errorf("WORKER_COUNT environment variable not set. It will be set to 1")
		WORKER_COUNT_STR = "1"
	}
	peers, err := strconv.Atoi(WORKER_COUNT_STR)
	unwrap(err, "Failed to convert WORKER_COUNT to int", log)

	receiverMoviesEOFs, err := middlewareConnection.ConsumeFrom("moviesEOFs", groupQueueName, "0", 1, uint(1))
	unwrap(err, "Failed to create channel for task", log)
	moviesEOFsChan := make(chan middleware.Envelope[*model.Row])

	subscribers := make([]string, peers)

	for i := range peers {
		if strings.Contains(currentTask.Name(), "ratings") {
			subscribers[i] = fmt.Sprintf("joiner_%d_ratings", i)
		} else {
			subscribers[i] = fmt.Sprintf("joiner_%d_credits", i)
		}
	}
	senderMoviesEOFs, err := middlewareConnection.WriteTo("moviesEOFs", subscribers, id, uint(peers))
	unwrap(err, "Failed to create channel for task", log)

	go func() {
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
	}()
	for {
		select {
		case <-ctx.Done():
			log.Infof("Received termination signal, shutting down gracefully...")
			currentTask.Finish()
			err = receiverMoviesEOFs.Close()
			if err != nil {
				log.Errorf("Failed to close receiver channel: %v", err)
			}
			err = senderMoviesEOFs.Close()
			if err != nil {
				log.Errorf("Failed to close receiver channel: %v", err)
			}
			close(moviesEOFsChan)
			err = middlewareConnection.Close()
			if err != nil {
				log.Errorf("Failed to close middleware connection: %v", err)
			}
			log.Infof("Graceful shutdown complete")
			return
		case envelope, ok = <-inputChannels[0]:
			if envelope != nil && envelope.Type() == middleware.EOF {
				log.Infof("Movies: EOF message received from %s", envelope.Cid())
				_, exists := clientsFinishedMovies[envelope.Cid()]
				if exists {
					log.Errorf("Client %s finished but already registered", envelope.Cid())
					err = envelope.Ack(false)
					unwrap(err, "Failed to ack EOF message", log)
					continue
				}
				clientsFinishedMovies[envelope.Cid()] = envelope
				senderMoviesEOFs.Send(&model.Row{}, envelope.Cid(), envelope.Id())
			} else if envelope != nil && envelope.Type() == middleware.Prune {
				log.Infof("Movies: Received prune message for client %s", envelope.Cid())
				err = envelope.Ack(false)
				unwrap(err, "Failed to ack prune message", log)
				continue
			} else if !ok {
				log.Infof("Channel closed 0, exiting...")
				closed++
				inputChannels[0] = nil
			}
		case envelope, ok = <-inputChannels[1]:
			if envelope != nil && envelope.Type() == middleware.EOF {
				log.Infof("Credits/Ratings: EOF message received from %s", envelope.Cid())
				_, exists := clientsFinishedInput[envelope.Cid()]
				if exists {
					log.Errorf("Client %s finished but already registered", envelope.Cid())
					err = envelope.Ack(false)
					unwrap(err, "Failed to ack EOF message", log)
					continue
				}
				clientsFinishedInput[envelope.Cid()] = envelope
				currentTask.ProcessPendingMovies(envelope.Cid())
			} else if envelope != nil && envelope.Type() == middleware.Prune {
				log.Infof("Credits/Ratings: Received prune message for client %s", envelope.Cid())
				err = envelope.Ack(false)
				unwrap(err, "Failed to ack prune message", log)
				continue

			} else if !ok {
				log.Infof("Channel closed 1, exiting...")
				inputChannels[1] = nil
				closed++
			}
		case envelopeEOF, ok = <-moviesEOFsChan:
			if ok && envelopeEOF != nil {
				_, exists := clientsFinishedMovies[envelopeEOF.Cid()]
				_, finished := clientsFinished[envelopeEOF.Cid()]
				if !exists && !finished {

					log.Infof("MoviesEOFs: EOF message received for %s", envelopeEOF.Cid())
					envelope = rabbitmq.NewEOFEnvelope[*model.Row](envelopeEOF.Cid(), nil, make(map[string]*amqp091.Delivery))
					clientsFinishedMovies[envelopeEOF.Cid()] = envelope
				}
				err = envelopeEOF.Ack(false)
				unwrap(err, "Failed to ack EOF message", log)
			}
		}

		if closed == 2 {
			break
		}
		if !ok && (envelope == nil || envelope.Type() == middleware.Normal) {
			continue
		}

		if envelope != nil && envelope.Type() == middleware.EOF {
			eofMovies, existsMovies := clientsFinishedMovies[envelope.Cid()]
			eofInput, existsInput := clientsFinishedInput[envelope.Cid()]
			if existsMovies && existsInput {
				log.Infof("Client %s finished", envelope.Cid())
				delete(clientsFinishedMovies, envelope.Cid())
				delete(clientsFinishedInput, envelope.Cid())
				if id == "0" {
					err = currentTask.FinishProcessingClient(envelope.Cid(), true)
				} else {
					err = currentTask.FinishProcessingClient(envelope.Cid(), false)
				}
				if err != nil {
					log.Errorf("Failed to finish processing client %s: %v", envelope.Cid(), err)
					continue
				}
				log.Infof("Finished processing client %s", envelope.Cid())
				err = eofMovies.Ack(false)
				if err != nil {
					log.Warnf("Failed to ack EOF message for movies: %v", err)
				}
				err = eofInput.Ack(false)
				unwrap(err, "Failed to ack EOF message", log)
				clientsFinished[envelope.Cid()] = true
			}
			continue

		}

		if envelope == nil || envelope.Msg() == nil {
			log.Warnf("Received nil message")
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
			log.Warnf("Failed to ack message: %v", err)
		}
		//unwrap(err, "Failed to ack message", log)
	}
	currentTask.Finish()
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
		Tasks: joiner_credits,
	}
}

func NewRatingsWorker(subscribers []string, id string, workerLogger *logger.ConsoleLogger) Worker {
	movies_metadata := NewSourceTask[*model.Row]("filter_release_date_ge_2000_and_include_ar")
	filter_avg_rating := NewSourceTask[*model.Row]("filter_avg_rating")

	joiner_ratings := ratings.NewJoinerRatings(movies_metadata, filter_avg_rating, subscribers, id, workerLogger)

	return Worker{
		Tasks: joiner_ratings,
	}
}
