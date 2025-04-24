package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/ptourne/sistemas-distribuidos-1/common"
	"github.com/ptourne/sistemas-distribuidos-1/common/logger"
	"github.com/ptourne/sistemas-distribuidos-1/middleware"

	//"github.com/ptourne/sistemas-distribuidos-1/worker/joiner"
	"github.com/ptourne/sistemas-distribuidos-1/worker/clean"
	"github.com/ptourne/sistemas-distribuidos-1/worker/task"
)

const MIDDLEWARE = "rabbitmq"

type Worker struct {
	TasksBin task.Task[[]byte, common.Row]
}

var WORKER_ID = os.Getenv("WORKER_ID")
var log = logger.NewConsoleLogger(fmt.Sprintf("worker_%s", WORKER_ID), logger.Debug)

type TType int

const (
	Bin TType = iota
	Row
)

func (w *Worker) Run() {
	middlewareConnection, err := middleware.NewRabbitmq[common.Row]()
	if err != nil {
		unwrap(err, "Failed to create middleware")
	}
	middlewareConnectionBin, err := middleware.NewRabbitmq[[]byte]()
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
		envelope, ok := <-inputChannels[0] 
		if !ok {
			log.Infof("Channel closed, exiting...")
			break
		}
		currentTask := w.TasksBin

		row := envelope.Msg()
		result := currentTask.ProcessAndSend(row)
		if result != nil {
			log.Errorf("Failed to process row: %v by task: %v", row, currentTask.Name())
			continue
		}
		err = envelope.Ack(false)
		unwrap(err, "Failed to ack message")
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

type SourceTask[O any] struct {
	name string
}

func NewSourceTask[O any](name string) task.Task[common.Row, O] {
	return &SourceTask[O]{name}
}

func (t *SourceTask[O]) ProcessAndSend(r common.Row) error {
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

func (t *SourceTask[O]) Connect(_ middleware.MiddlewareCola[common.Row], _ middleware.MiddlewareCola[O]) ([]chan middleware.Envelope[common.Row], error) {
	return nil, nil
}

func NewWorker() Worker {
	ratings := NewSourceTask[[]byte]("ratings")
	
	ratings_clean := clean.NewCleanRatings(ratings, []string{"reduce_by_movieId"})
	return Worker{
		TasksBin: ratings_clean,
	}
}
