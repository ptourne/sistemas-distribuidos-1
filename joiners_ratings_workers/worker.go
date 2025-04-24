package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/ptourne/sistemas-distribuidos-1/common"
	"github.com/ptourne/sistemas-distribuidos-1/common/logger"
	"github.com/ptourne/sistemas-distribuidos-1/middleware"

	"github.com/ptourne/sistemas-distribuidos-1/worker/joiner"
	"github.com/ptourne/sistemas-distribuidos-1/worker/task"
)

const MIDDLEWARE = "rabbitmq"

type Worker struct {
	Tasks task.Task[common.Row, common.Row]
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

	log.Infof("Connected to middleware: %s", MIDDLEWARE)

	inputChannels, err := w.Tasks.Connect(middlewareConnection, middlewareConnection)
	if err != nil {
		log.Fatalf("Failed to create channel for task %s: %s", w.Tasks.Name(), err)
	}
	log.Infof("Connected to task %s", w.Tasks.Name())
	closed := 0
	var envelope middleware.Envelope[common.Row]
	var ok bool
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

		currentTask := w.Tasks
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
	movies_metadata := NewSourceTask[common.Row]("filter_release_date_ge_2000_and_include_ar")
	ratings := NewSourceTask[common.Row]("filter_avg_rating")

	joiner_ratings := joiner.NewJoinerRatings(movies_metadata, ratings, []string{"q3"})

	return Worker{
		Tasks: joiner_ratings,
	}
}
