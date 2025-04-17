package main

import (
	"fmt"
	"os"
	"reflect"

	"github.com/ptourne/sistemas-distribuidos-1/common"
	"github.com/ptourne/sistemas-distribuidos-1/common/logger"
	"github.com/ptourne/sistemas-distribuidos-1/middleware"
	"github.com/ptourne/sistemas-distribuidos-1/worker/clean"
	"github.com/ptourne/sistemas-distribuidos-1/worker/filter"
	"github.com/ptourne/sistemas-distribuidos-1/worker/task"
)
const MIDDLEWARE = "rabbitmq"

type Worker struct {
	Tasks []task.Task
}

var WORKER_ID = os.Getenv("WORKER_ID")
var log = logger.NewConsoleLogger(fmt.Sprintf("worker_%s", WORKER_ID), logger.Debug)

func (w Worker) Run() {
	middlewareChan, err1 := middleware.NewMiddleware(MIDDLEWARE)
	if err1 != nil {
		unwrap(err1, "Failed to create middleware")
	}
	log.Infof("Connected to middleware: %s", MIDDLEWARE)
	defer middlewareChan.Close()

	cases := make([]reflect.SelectCase, len(w.Tasks))
	senders:= make([]middleware.Sender, len(w.Tasks))
	for i, task := range w.Tasks {
		taskReceiver, err := middlewareChan.CreateReadQueue(task.Input(), task.Name())
		if err != nil {
			unwrap(err, "Failed to create read queue for task" + task.Name())
		}
		taskSender, err := middlewareChan.CreateWriteQueue(task.Name())
		if err != nil {
			unwrap(err, "Failed to create write queue for task" + task.Name())
		}

		inputChannel := make(chan *common.Row)
		go func() {
			for {
				row, err := taskReceiver.Next(nil)  
				if err != nil {
					if err.Error() == "read channel was closed"{
						log.Infof("Channel closed: %v", task.Name())
						break
					}
					log.Errorf("Error reading from middleware: %v", err)
					continue 
				}
				inputChannel <- row
			}
		}()

		cases[i] = reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(inputChannel),
		}
		senders[i] = taskSender
	}

	for {
		i, val, ok := reflect.Select(cases)
		if !ok {
			panic("Channel closed")
		}
		log.Infof("Received message from channel %d", i)
		row, ok := val.Interface().(*common.Row)
		if !ok {
			panic("Failed to cast to amqp.Delivery")
		}
		unwrap(err1, "Failed to unmarshal JSON")
		task := w.Tasks[i]
		sender := senders[i]
		result := task.Process(*row)
		if result == nil {
			log.Infof("Row filtered out: %v name: %v", row.Strings["title"], task.Name())
			continue
		}
		err2 := sender.Send(result)
		unwrap(err2, "Failed to publish a message")
		// err3 := delivery.Ack(false) TODOOOO!!!!
		// unwrap(err3, "Failed to ack message")
	}
}

func unwrap(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
		panic(err)
	}
}

type SourceTask struct {
	name string
}

func NewSourceTask(name string) task.Task {
	return &SourceTask{name}
}

func (t *SourceTask) Process(r common.Row) *common.Row {
	return nil
}

func (t *SourceTask) Name() string {
	return t.name
}

func (t *SourceTask) Input() string {
	return ""
}

func NewWorker() Worker {
	movies_metadata := NewSourceTask("movies_metadata")
	movies_metadata_clean := clean.NewCleanMovies(movies_metadata)
	filter_release_date_ge_2000_and_include_ar := filter.NewFilterReleaseDateGe2000AndIncludeAR(movies_metadata_clean)
	filter_release_date_l_2010_and_include_es := filter.NewFilterReleaseDateL2010AndIncludeES(filter_release_date_ge_2000_and_include_ar)
	filter_one_production_country := filter.NewFilterProductionCountriesLen1(movies_metadata_clean)
	return Worker{
		Tasks: []task.Task{
			movies_metadata_clean,
			filter_release_date_ge_2000_and_include_ar,
			filter_release_date_l_2010_and_include_es,
			filter_one_production_country,
		},
	}
}
