package main

import (
	"fmt"
	"os"
	"reflect"
	"slices"

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
var log = logger.NewConsoleLogger(fmt.Sprintf("worker_%s", WORKER_ID), logger.Info)

func (w Worker) Run() {
	middlewareConnection, err := middleware.NewRabbitmq[common.Row]()
	if err != nil {
		unwrap(err, "Failed to create middleware")
	}
	log.Infof("Connected to middleware: %s", MIDDLEWARE)
	defer middlewareConnection.Close()
	cases := make([]reflect.SelectCase, len(w.Tasks))
	for i, task := range w.Tasks {
		inputChannel, err := task.Connect(middlewareConnection)
		if err != nil {
			log.Fatalf("Failed to create channel for task %s: %s", task.Name(), err)
		}

		cases[i] = reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(inputChannel),
		}
	}

	for {
		if len(cases) == 0 {
			log.Infof("All channels closed")
			break
		}
		i, val, ok := reflect.Select(cases)
		currentTask := w.Tasks[i]
		if !ok {
			log.Infof("Channel closed: %s", currentTask.Name())
			cases = slices.Delete(cases, i, i+1)
			w.Tasks = slices.Delete(w.Tasks, i, i+1)
			currentTask.Finish()
			continue
		}
		log.Debugf("Received message from channel %d", i)
		envelope, ok := val.Interface().(middleware.Envelope[common.Row])
		if !ok {
			panic("Failed to cast to envelope")
		}
		row := envelope.Msg()
		err := currentTask.ProcessAndSend(row)
		if err != nil {
			log.Errorf("Failed to process message: %s", err)
			continue
		}
		log.Debugf("TO ACK msg %v worker", envelope.Msg())
		err = envelope.Ack(false)
		unwrap(err, "Failed to ack message")
		log.Debugf("Row processed: %v name: %v", row.Strings["title"], currentTask.Name())
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

func (t *SourceTask) ProcessAndSend(r common.Row) error {
	return nil
}

func (t *SourceTask) Name() string {
	return t.name
}

func (t *SourceTask) Input() string {
	return ""
}

func (t *SourceTask) Finish() error {
	return nil
}

func (t *SourceTask) Connect(middlewareConnection middleware.MiddlewareCola[common.Row]) (chan middleware.Envelope[common.Row], error) {
	return nil, nil
}

func NewWorker() Worker {
	movies_metadata := NewSourceTask("movies_metadata")
	movies_metadata_clean := clean.NewCleanMovies(movies_metadata)
	filter_release_date_ge_2000_and_include_ar := filter.NewFilterReleaseDateGe2000AndIncludeAR(movies_metadata_clean, []string{"filter_release_date_l_2010_and_include_es"})
	filter_release_date_l_2010_and_include_es := filter.NewFilterReleaseDateL2010AndIncludeES(filter_release_date_ge_2000_and_include_ar, []string{"q1"})
	filter_one_production_country := filter.NewFilterProductionCountriesLen1(movies_metadata,[]string{"reduce_by_country_sum_budget" , "q1f"})
	return Worker{
		Tasks: []task.Task{
			movies_metadata_clean,
			filter_release_date_ge_2000_and_include_ar,
			filter_release_date_l_2010_and_include_es,
			filter_one_production_country,
		},
	}
}
