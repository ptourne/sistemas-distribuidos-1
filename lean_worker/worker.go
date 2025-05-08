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

	//"github.com/ptourne/sistemas-distribuidos-1/worker/joiner"
	"github.com/ptourne/sistemas-distribuidos-1/worker/clean"
	"github.com/ptourne/sistemas-distribuidos-1/worker/task"
)

const MIDDLEWARE = "rabbitmq"

type Worker struct {
	TasksBin task.Task[*model.FileChunk, *model.Row]
}

var WORKER_ID = os.Getenv("WORKER_ID")
var log = logger.NewConsoleLogger(fmt.Sprintf("worker_%s", WORKER_ID), logger.Info)

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
	middlewareConnection := rabbitmq.NewMiddleware[*model.Row](connector, log)
	middlewareConnectionBin := rabbitmq.NewMiddleware[*model.FileChunk](connector, log)
	if err != nil {
		unwrap(err, "Failed to create middleware")
	}
	log.Infof("Connected to middleware: %s", MIDDLEWARE)
	defer middlewareConnectionBin.Close()

	inputChannels, err := w.TasksBin.Connect(middlewareConnectionBin, middlewareConnection)
	if err != nil {
		log.Fatalf("Failed to create channel for task %s: %s", w.TasksBin.Name(), err)
	}
	log.Infof("Connected to task %s", w.TasksBin.Name())
	for {
		currentTask := w.TasksBin

		envelope, ok := <-inputChannels[0]
		if !ok {
			log.Infof("Channel closed, exiting...")
			currentTask.Finish()
			break
		}

		result := currentTask.ProcessAndSend(envelope)
		if result != nil {
			log.Errorf("Failed to process row: %v by task: %v", envelope.Msg(), currentTask.Name())
			continue
		}
		err = envelope.Ack(false)
		unwrap(err, "Failed to ack message in run")
	}
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

func (t *SourceTask[O]) ProcessAndSend(e middleware.Envelope[*model.Row]) error {
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
	ratings := NewSourceTask[*model.FileChunk]("ratings")
	subscribers := map[string][]string{
		"reduce_by_movieId": []string{"0", "1", "2", "3", "4", "5", "6", "7", "8", "9"},
	}
	ratings_clean := clean.NewCleanRatings(ratings, subscribers)
	return Worker{
		TasksBin: ratings_clean,
	}
}
