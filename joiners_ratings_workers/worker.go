package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/ptourne/sistemas-distribuidos-1/common/logger"
	"github.com/ptourne/sistemas-distribuidos-1/common/model"
	"github.com/ptourne/sistemas-distribuidos-1/middleware/codec"
	"github.com/ptourne/sistemas-distribuidos-1/middleware/middleware"
	"github.com/ptourne/sistemas-distribuidos-1/middleware/middleware/rabbitmq"

	"github.com/ptourne/sistemas-distribuidos-1/worker/joiner"
	"github.com/ptourne/sistemas-distribuidos-1/worker/task"
)

const MIDDLEWARE = "rabbitmq"

type Worker struct {
	Tasks task.Task[*model.Row, *model.Row]
}

var WORKER_ID = os.Getenv("WORKER_ID")
var log = logger.NewConsoleLogger(fmt.Sprintf("worker_%s", WORKER_ID), logger.Debug)

type TType int

const (
	Bin TType = iota
	Row
)

func (w *Worker) Run() {
	connector, err := rabbitmq.Connector()
	if err != nil {
		log.Fatalf("Failed to connect to middleware: %s", err)
	}
	middlewareLog := logger.NewConsoleLogger("middleware", logger.Info)

	middlewareConnection := rabbitmq.NewMiddleware[*model.Row](connector, middlewareLog)

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
	for {
		select {
		case envelope, ok = <-inputChannels[0]:
			if !ok {
				log.Infof("Channel closed 0, exiting...")
				closed++
				inputChannels[0] = nil
			}
		case envelope, ok = <-inputChannels[1]:
			if !ok {
				log.Infof("Channel closed 1, exiting...")
				inputChannels[1] = nil
				closed++
			}
			log.Infof("Channel 1 MSG")
		}

		if closed == 2 {
			break
		}
		if !ok {
			continue
		}

		row := envelope.Msg()
		result := currentTask.ProcessAndSend(row)
		if result != nil {
			log.Errorf("Failed to process row: %v by task: %v", row, currentTask.Name())
			continue
		}
		log.Debugf("TO ACK msg %v worker", envelope.Msg())
		err = envelope.Ack(false)
		unwrap(err, "Failed to ack message")
	}
	currentTask.Finish()
}

func unwrap(err error, msg string) {
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

func NewWorker() Worker {
	movies_metadata := NewSourceTask[*model.Row]("filter_release_date_ge_2000_and_include_ar")
	ratings := NewSourceTask[*model.Row]("filter_avg_rating")

	joiner_ratings := joiner.NewJoinerRatings(movies_metadata, ratings, []string{"reduce_top_bottom_avg_rating"})

	return Worker{
		Tasks: joiner_ratings,
	}
}
