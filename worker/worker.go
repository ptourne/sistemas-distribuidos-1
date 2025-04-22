package main

import (
	"fmt"
	"os"
	"reflect"
	"slices"
	"strconv"
	"strings"

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

func (w *Worker) Run() {
	middlewareConnection, err := middleware.NewRabbitmq[common.Row]()
	if err != nil {
		unwrap(err, "Failed to create middleware")
	}
	log.Infof("Connected to middleware: %s", MIDDLEWARE)
	defer middlewareConnection.Close()
	var cases []reflect.SelectCase
	//cases := make([]reflect.SelectCase, len(w.Tasks))
	var taskRefs []task.Task
	taskClosedChannels := map[string]int{}
	taskChannelCounts := map[string]int{}

	for _, task := range w.Tasks {
		inputChannels, err := task.Connect(middlewareConnection)
		if err != nil {
			log.Fatalf("Failed to create channel for task %s: %s", task.Name(), err)
		}

		taskChannelCounts[task.Name()] = len(inputChannels)
		taskClosedChannels[task.Name()] = 0

		for _, ch := range inputChannels {
			cases = append(cases, reflect.SelectCase{
				Dir:  reflect.SelectRecv,
				Chan: reflect.ValueOf(ch),
			})
			taskRefs = append(taskRefs, task)
		}

		// cases[i] = reflect.SelectCase{
		// 	Dir:  reflect.SelectRecv,
		// 	Chan: reflect.ValueOf(inputChannel),
		// }
	}

	for {
		if len(cases) == 0 {
			log.Infof("All channels closed")
			break
		}
		i, val, ok := reflect.Select(cases)
		//currentTask := w.Tasks[i]
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
				//currentTask.Finish()

			}
			continue
		}

		log.Debugf("Received message from channel %d", i)
		envelope, ok := val.Interface().(middleware.Envelope[common.Row])
		if !ok {
			panic("Failed to cast to envelope")
		}
		row := envelope.Msg()
		result := currentTask.ProcessAndSend(row)
		if result != nil {
			log.Errorf("Failed to process row: %v by task: %v", row, currentTask.Name())
			continue
		}
		err = envelope.Ack(false)
		unwrap(err, "Failed to ack message")
		log.Debugf("Row processed: %v name: %v", row.Strings["title"], currentTask.Name())
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

func (t *SourceTask) Connect(middlewareConnection middleware.MiddlewareCola[common.Row]) ([]chan middleware.Envelope[common.Row], error) {
	return nil, nil
}

func NewWorker() Worker {
	movies_metadata := NewSourceTask("movies_metadata")
	// credits := NewSourceTask("credits")
	// ratings := NewSourceTask("ratings")
	movies_metadata_clean := clean.NewCleanMovies(movies_metadata, []string{"filter_release_date_ge_2000_and_include_ar", "filter_one_production_country", "map_sentiment_rate"})
	n_worker, err := strconv.Atoi(os.Getenv("N_JOINERS")) // TODO: cambiar en el compose
	if err != nil {
		log.Fatalf("Failed to convert N_JOINERS to int: %s", err)
	}
	var joiner_credits_subscribers []string
	var joiner_ratings_subscribers []string
	for i := range n_worker {
		joiner_credits_subscribers = append(joiner_credits_subscribers, fmt.Sprintf("joiner_%d_credits", i+1))
		joiner_ratings_subscribers = append(joiner_ratings_subscribers, fmt.Sprintf("joiner_%d_ratings", i+1))
	}
	// credits_clean := clean.NewCleanCredits(credits, joiner_credits_subscribers)
	// ratings_clean := clean.NewCleanRatings(ratings, joiner_ratings_subscribers)

	// filter_release_date_ge_2000_and_include_ar := filter.NewFilterReleaseDateGe2000AndIncludeAR(movies_metadata_clean, []string{"filter_release_date_l_2010_and_include_es"})
	// filter_release_date_l_2010_and_include_es := filter.NewFilterReleaseDateL2010AndIncludeES(filter_release_date_ge_2000_and_include_ar, []string{"q1"})
	// filter_one_production_country := filter.NewFilterProductionCountriesLen1(movies_metadata_clean, []string{})
	// joiner_credits := joiner.NewJoinerCredits(filter_release_date_ge_2000_and_include_ar, credits_clean, []string{})
	// joiner_ratings := joiner.NewJoinerRatings(filter_release_date_ge_2000_and_include_ar, ratings_clean, []string{})

	//grpcAddress := "192.168.1.100:50051"
	grpcAddress := os.Getenv("NLP_GRPC_ADDR")

	map_nlp := filter.NewFilterSentimentAndRate(movies_metadata_clean, []string{}, grpcAddress)

	return Worker{
		Tasks: []task.Task{
			movies_metadata_clean,
			// ratings_clean,
			// credits_clean,
			// filter_release_date_ge_2000_and_include_ar,
			// filter_release_date_l_2010_and_include_es,
			// filter_one_production_country,
			// joiner_credits,
			// joiner_ratings,
			map_nlp,
		},
	}
}
