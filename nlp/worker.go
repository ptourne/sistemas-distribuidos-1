package main

import (
	"fmt"
	"os"
	"reflect"
	"slices"
	"strings"

	"github.com/ptourne/sistemas-distribuidos-1/common/logger"
	"github.com/ptourne/sistemas-distribuidos-1/common/model"
	"github.com/ptourne/sistemas-distribuidos-1/middleware/middleware"
	"github.com/ptourne/sistemas-distribuidos-1/middleware/middleware/rabbitmq"

	"github.com/ptourne/sistemas-distribuidos-1/worker/filter"
	"github.com/ptourne/sistemas-distribuidos-1/worker/task"
)

const MIDDLEWARE = "rabbitmq"

type Worker struct {
	Tasks []task.Task[*model.Row, *model.Row]
}

var WORKER_ID = os.Getenv("WORKER_ID")
var log = logger.NewConsoleLogger(fmt.Sprintf("worker_%s", WORKER_ID), logger.Info)

type TType int

const (
	Bin TType = iota
	Row
)

func (w *Worker) Run() {
	connector, err := rabbitmq.Connector("reduce-nlp")
	if err != nil {
		log.Fatalf("Failed to connect to middleware: %s", err)
	}
	middlewareLog := logger.NewConsoleLogger("middleware", logger.Info)
	middlewareConnection := rabbitmq.NewMiddleware[*model.Row](connector, middlewareLog)
	middlewareConnectionBin := rabbitmq.NewMiddleware[*model.FileChunk](connector, middlewareLog)
	if err != nil {
		unwrap(err, "Failed to create middleware")
	}
	log.Infof("Connected to middleware: %s", MIDDLEWARE)
	defer middlewareConnection.Close()
	defer middlewareConnectionBin.Close()
	var cases []reflect.SelectCase
	var taskRefs []task.Task[*model.Row, *model.Row]

	taskClosedChannels := map[string]int{}
	taskChannelCounts := map[string]int{}
	for _, taski := range w.Tasks {
		inputChannels, err := taski.Connect(middlewareConnection, middlewareConnection)
		if err != nil {
			log.Fatalf("Failed to create channel for task %s: %s", taski.Name(), err)
		}

		taskChannelCounts[taski.Name()] = len(inputChannels)
		taskClosedChannels[taski.Name()] = 0

		for _, ch := range inputChannels {
			cases = append(cases, reflect.SelectCase{
				Dir:  reflect.SelectRecv,
				Chan: reflect.ValueOf(ch),
			})
			taskRefs = append(taskRefs, taski)
		}
	}

	for {
		if len(cases) == 0 {
			log.Infof("All channels closed")
			break
		}
		i, val, ok := reflect.Select(cases)

		currentTask := taskRefs[i]
		if !ok {

			log.Infof("Channel closed from task: %s", currentTask.Name())
			cases = slices.Delete(cases, i, i+1)
			taskClosedChannels[currentTask.Name()]++
			taskRefs = slices.Delete(taskRefs, i, i+1)
			if taskClosedChannels[currentTask.Name()] == taskChannelCounts[currentTask.Name()] {
				log.Infof("All channels closed for task: %s", currentTask.Name())
				currentTask.Finish()
				for j, task := range w.Tasks {

					if task.Name() == currentTask.Name() {
						w.Tasks = slices.Delete(w.Tasks, j, j+1)
						break
					}
				}

			}
			continue
		}

		log.Debugf("Received message from channel %d", i)
		envelope, ok := val.Interface().(middleware.Envelope[*model.Row])
		if !ok {
			panic("Failed to cast to envelope")
		}
		result := currentTask.ProcessAndSend(envelope)
		if result != nil {
			log.Errorf("Failed to process row: %v by task: %v", envelope, currentTask.Name())
			continue
		}
		// log.Debugf("TO ACK msg %v worker", envelope.Msg())
		err = envelope.Ack(false)
		unwrap(err, "Failed to ack message")
		// log.Debugf("Row processed: %v name: %v", row.Strings["title"], currentTask.Name())

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

func NewWorker() Worker {

	grpcAddress := os.Getenv("NLP_GRPC_ADDR")

	map_nlp := filter.NewFilterSentimentAndRate("clean_movies", []string{"reduce_by_sentiment"}, grpcAddress)

	return Worker{
		Tasks: []task.Task[*model.Row, *model.Row]{
			map_nlp,
		},
	}
}
