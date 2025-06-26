package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"syscall"

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

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	for {
		select {
		case <-ctx.Done():
			log.Infof("Received termination signal, shutting down gracefully...")
			for _, task := range w.Tasks {
				err := task.Finish()
				if err != nil {
					log.Errorf("Failed to finish task %s: %s", task.Name(), err)
				}
			}
			return
		default:
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
				errNack := envelope.Nack(false)
				if errNack != nil {
					log.Errorf("Failed to nack message")
				}
				panic("Nack")
				//continue
			}
			// log.Debugf("TO ACK msg %v worker", envelope.Msg())
			err = envelope.Ack(false)
			unwrap(err, "Failed to ack message")
			// log.Debugf("Row processed: %v name: %v", row.Strings["title"], currentTask.Name())
		}
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
	var addrs []string
	grpcAddresses := os.Getenv("NLP_GRPC_ADDRS")
	for _, addr := range strings.Split(grpcAddresses, ",") {
		addrs = append(addrs, strings.TrimSpace(addr))
	}

	nWorkersStr := os.Getenv("N_WORKERS")
	if nWorkersStr == "" {
		nWorkersStr = "1"
	}
	nWorkers, err := strconv.Atoi(nWorkersStr)
	if err != nil {
		log.Fatalf("Failed to parse N_WORKERS: %s", err)
	}

	nConsumersStr := os.Getenv("CONSUMER_COUNT")
	if nConsumersStr == "" {
		nConsumersStr = "1"
	}
	nConsumerCount, err := strconv.Atoi(nConsumersStr)
	if err != nil {
		log.Fatalf("Failed to parse CONSUMER_COUNT: %s", err)
	}

	map_nlp := filter.NewFilterSentimentAndRate("clean_movies", []string{"reduce_by_sentiment"}, addrs, uint(nConsumerCount), uint(nWorkers))

	return Worker{
		Tasks: []task.Task[*model.Row, *model.Row]{
			map_nlp,
		},
	}
}
