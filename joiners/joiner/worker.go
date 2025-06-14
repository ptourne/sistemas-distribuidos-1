package joiner

import (
	"context"
	"os/signal"
	"syscall"

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
	ClientsPruneMovies    map[uint64]middleware.Envelope[*model.Row]
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

	for {
		select {
		case <-ctx.Done():
			w.shutdown(middlewareConnection, log)
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

			} else if envelope != nil && envelope.Type() == middleware.Prune {
				if _, finished := w.ClientsFinished[envelope.Cid()]; finished {
					err = envelope.Ack(false)
					if err != nil {
						log.Warnf("Failed to ack prune message for movies: %v", err)
					}
					continue
				}
				if id == "0" {
					err = envelope.ResendIfRedelivered()
					if err != nil {
						log.Warnf("Failed to resend eofcid message for movies: %v", err)
					}
				}
				log.Infof("Movies: Received prune message for client %d", envelope.Cid())
				oldPrune, exists := w.ClientsPruneMovies[envelope.Cid()]
				w.ClientsPruneMovies[envelope.Cid()] = envelope
				if exists {
					err = oldPrune.Ack(false)
					if err != nil {
						log.Warnf("Failed to ack prune message for movies: %v", err)
					}
					continue
				}
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
		}

		if closed == 2 {
			break
		}
		if !ok && (envelope == nil || envelope.Type() == middleware.Normal) {
			continue
		}

		if envelope != nil && envelope.Type() == middleware.EOF || envelope.Type() == middleware.Prune {
			w.processEOF(envelope, id, log, currentTask)
			continue

		}

		if envelope == nil || envelope.Msg() == nil {
			log.Warnf("Received nil message: %+v", envelope)
			continue
		}

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

func (w *Worker) shutdown(middlewareConnection middleware.Connection[*model.Row], log *logger.ConsoleLogger) { //, receiverMoviesEOFs middleware.Receiver[*model.Row], senderMoviesEOFs middleware.Sender[*model.Row], moviesEOFsChan chan middleware.Envelope[*model.Row]
	log.Infof("Received termination signal, shutting down gracefully...")
	var err error
	currentTask := w.Tasks
	currentTask.Finish()
	err = middlewareConnection.Close()
	if err != nil {
		log.Errorf("Failed to close middleware connection: %v", err)
	}
	log.Infof("Graceful shutdown complete")
}

func (w *Worker) processEOF(envelope middleware.Envelope[*model.Row], id string, log *logger.ConsoleLogger, currentTask task.JoinerTask[*model.Row, *model.Row]) {
	var err error
	eofMovies, existsMovies := w.ClientsFinishedMovies[envelope.Cid()]
	pruneMovies, existsPrune := w.ClientsPruneMovies[envelope.Cid()]
	eofInput, existsInput := w.ClientsFinishedInput[envelope.Cid()]
	if existsPrune && existsInput {
		log.Infof("Client %d finished", envelope.Cid())
		if id != "0" {
			currentTask.FinishProcessingClient(envelope.Cid(), false)
			err = eofInput.Ack(false)
			if err != nil {
				log.Warnf("Failed to ack EOF message for credits/ratings: %v", err)
			}
		}
		err = pruneMovies.Ack(false)
		if err != nil {
			log.Warnf("Failed to ack prune message for movies: %v", err)
		}
		w.ClientsFinished[envelope.Cid()] = true
		delete(w.ClientsPruneMovies, envelope.Cid())

	}
	if existsMovies && existsInput {
		if id == "0" {
			currentTask.FinishProcessingClient(envelope.Cid(), true)
		}
		log.Infof("Finished processing client %d", envelope.Cid())
		err = eofMovies.Ack(false)
		if err != nil {
			log.Warnf("Failed to ack EOF message for movies: %v", err)
		}
		err = eofInput.Ack(false)
		if err != nil {
			log.Warnf("Failed to ack EOF message for credits/ratings: %v", err)
		}
		delete(w.ClientsFinishedMovies, envelope.Cid())
		delete(w.ClientsFinishedInput, envelope.Cid())
		// delete(w.ClientsPruneMovies, envelope.Cid())
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
		ClientsPruneMovies:    make(map[uint64]middleware.Envelope[*model.Row]),
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
		ClientsPruneMovies:    make(map[uint64]middleware.Envelope[*model.Row]),
		ClientsFinishedInput:  make(map[uint64]middleware.Envelope[*model.Row]),
		ClientsFinished:       make(map[uint64]bool),
	}
}
