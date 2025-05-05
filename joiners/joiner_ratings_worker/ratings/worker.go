package ratings

import (
	"fmt"
	"os"
	"strings"

	"github.com/ptourne/sistemas-distribuidos-1/common/logger"
	"github.com/ptourne/sistemas-distribuidos-1/common/model"
	"github.com/ptourne/sistemas-distribuidos-1/middleware/codec"
	"github.com/ptourne/sistemas-distribuidos-1/middleware/middleware"

	"github.com/ptourne/sistemas-distribuidos-1/worker/task"
)

const MIDDLEWARE = "rabbitmq"

type Worker struct {
	Tasks task.JoinerTask[*model.Row, *model.Row]
}

var WORKER_ID = os.Getenv("WORKER_ID")
var Log = logger.NewConsoleLogger(fmt.Sprintf("worker_%s", WORKER_ID), logger.Debug)

type TType int

const (
	Bin TType = iota
	Row
)

func (w *Worker) Run(middlewareConnection middleware.Connection[*model.Row]) {
	Log.Infof("Connected to middleware: %s", MIDDLEWARE)

	inputChannels, err := w.Tasks.Connect(middlewareConnection, middlewareConnection)
	if err != nil {
		Log.Fatalf("Failed to create channel for task %s: %s", w.Tasks.Name(), err)
	}
	Log.Infof("Connected to task %s", w.Tasks.Name())
	closed := 0
	clientsFinished := make(map[string][]middleware.Envelope[*model.Row])
	clientsPrune := make(map[string][]middleware.Envelope[*model.Row])
	var envelope middleware.Envelope[*model.Row]
	var ok bool
	currentTask := w.Tasks
	for {
		select {
		case envelope, ok = <-inputChannels[0]:
			if envelope != nil && envelope.Type() == middleware.EOF {
				eofMsg, exists := clientsFinished[envelope.Cid()]
				if !exists {
					eofMsg = []middleware.Envelope[*model.Row]{}
				}
				clientsFinished[envelope.Cid()] = append(eofMsg, envelope)
			} else if !ok {
				Log.Infof("Channel closed 0, exiting...")
				closed++
				inputChannels[0] = nil
			}
		case envelope, ok = <-inputChannels[1]:
			if envelope != nil && envelope.Type() == middleware.EOF {
				eofMsg, exists := clientsFinished[envelope.Cid()]
				if !exists {
					eofMsg = []middleware.Envelope[*model.Row]{}
				}
				clientsFinished[envelope.Cid()] = append(eofMsg, envelope)
				currentTask.ProcessPendingMovies(envelope.Cid())
			} else if !ok {
				Log.Infof("Channel closed 1, exiting...")
				inputChannels[1] = nil
				closed++
			}
		}

		if closed == 2 {
			break
		}
		if !ok && (envelope == nil || envelope.Type() != middleware.EOF) {
			continue
		}

		if envelope != nil && envelope.Type() == middleware.Prune {
			pruneMsgs, exists := clientsPrune[envelope.Cid()]
			if !exists {
				clientsPrune[envelope.Cid()] = []middleware.Envelope[*model.Row]{}
			}
			clientsPrune[envelope.Cid()] = append(clientsPrune[envelope.Cid()], envelope)
			if len(clientsPrune[envelope.Cid()]) == 2 {
				Log.Infof("Client %s prune", envelope.Cid())
				delete(clientsPrune, envelope.Cid())
				for _, pruneMsg := range pruneMsgs {
					err = pruneMsg.Ack(false)
					unwrap(err, "Failed to ack prune message")
				}
			}
			continue
		}

		if envelope != nil && envelope.Type() == middleware.EOF {
			eofMsgs, exists := clientsFinished[envelope.Cid()]
			if !exists {
				Log.Errorf("Client %s finished but not registered", envelope.Cid())
				continue
			}
			if len(eofMsgs) == 2 {
				Log.Infof("Client %s finished", envelope.Cid())
				delete(clientsFinished, envelope.Cid())
				err = currentTask.FinishProcessingClient(envelope.Cid())
				if err != nil {
					Log.Errorf("Failed to finish processing client %s: %v", envelope.Cid(), err)
					continue
				}
				Log.Infof("Finished processing client %s", envelope.Cid())
				for _, msg := range eofMsgs {
					msg.Ack(false)
				}
			}
			continue
		}

		row := envelope.Msg()
		row.Strings["cid"] = envelope.Cid()
		result := currentTask.ProcessAndSend(row)
		if result != nil {
			Log.Errorf("Failed to process row: %v by task: %v", row, currentTask.Name())
			continue
		}
		Log.Debugf("TO ACK msg %v worker", envelope.Msg())
		err = envelope.Ack(false)
		unwrap(err, "Failed to ack message")
	}
	currentTask.Finish()
}

func unwrap(err error, msg string) {
	if err != nil {
		if strings.Contains(err.Error(), "channel/connection is not open") {
			Log.Warnf("%s: %s", msg, err)
		} else {
			Log.Fatalf("%s: %s", msg, err)
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

func (t *SourceTask[O]) ProcessAndSend(r *model.Row) error {
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

func NewWorker(subscribers []string) Worker {
	movies_metadata := NewSourceTask[*model.Row]("filter_release_date_ge_2000_and_include_ar")
	ratings := NewSourceTask[*model.Row]("filter_avg_rating")

	joiner_ratings := NewJoinerRatings(movies_metadata, ratings, subscribers)

	return Worker{
		Tasks: joiner_ratings,
	}
}
