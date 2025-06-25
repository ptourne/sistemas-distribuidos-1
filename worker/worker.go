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
	"github.com/ptourne/sistemas-distribuidos-1/middleware/codec"
	"github.com/ptourne/sistemas-distribuidos-1/middleware/middleware"
	"github.com/ptourne/sistemas-distribuidos-1/middleware/middleware/rabbitmq"

	"github.com/ptourne/sistemas-distribuidos-1/worker/clean"
	"github.com/ptourne/sistemas-distribuidos-1/worker/filter"

	"github.com/ptourne/sistemas-distribuidos-1/worker/task"
)

const MIDDLEWARE = "rabbitmq"

type Worker struct {
	TasksBin []task.Task[*model.FileChunk, *model.Row]
	Tasks    []task.Task[*model.Row, *model.Row]
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
	//cases := make([]reflect.SelectCase, len(w.Tasks))
	var taskRefs []task.Task[*model.Row, *model.Row]
	var taskBinRefs []task.Task[*model.FileChunk, *model.Row]

	taskClosedChannels := map[string]int{}
	taskChannelCounts := map[string]int{}
	for _, taski := range w.TasksBin {
		inputChannels, err := taski.Connect(middlewareConnectionBin, middlewareConnection)
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
			taskBinRefs = append(taskBinRefs, taski)
		}
	}
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
	cantBin := len(w.TasksBin)
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
Output:
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
			for _, task := range w.TasksBin {
				err := task.Finish()
				if err != nil {
					log.Errorf("Failed to finish task %s: %s", task.Name(), err)
				}
			}
			return
		default:
			if len(cases) == 0 {
				log.Infof("All channels closed")
				break Output
			}
			i, val, ok := reflect.Select(cases)
			if i < cantBin {
				currentTask := taskBinRefs[i]
				if !ok {
					log.Infof("Channel closed from task: %s", currentTask.Name())
					cases = slices.Delete(cases, i, i+1)
					taskClosedChannels[currentTask.Name()]++
					taskRefs = slices.Delete(taskRefs, i, i+1)
					cantBin--
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
				envelope, ok := val.Interface().(middleware.Envelope[*model.FileChunk])
				if !ok {
					panic("Failed to cast to envelope")
				}

				err = currentTask.ProcessAndSend(envelope)
				if err != nil {
					log.Errorf("Failed to process fileChunk: %v by task: %v. Error: %s", envelope, currentTask.Name(), err)
					errNack := envelope.Nack(false)
					unwrap(errNack, fmt.Sprintf("Failed to nack message: %v. Error: %s", envelope, err))
					panic(fmt.Sprintf("Failed to process fileChunk: %v by task: %v. Error: %s", envelope, currentTask.Name(), err))
				}
				err = envelope.Ack(false)
				unwrap(err, "Failed to ack message")
			} else {

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
								break Output
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
				err = currentTask.ProcessAndSend(envelope)
				if err != nil {
					log.Errorf("Failed to process row: %v by task: %v", envelope, currentTask.Name())
					errNack := envelope.Nack(false)
					unwrap(errNack, fmt.Sprintf("Failed to nack message: %v. Error: %s", envelope, err))
					panic(fmt.Sprintf("Failed to process row: %v by task: %v. Error: %s", envelope, currentTask.Name(), err))
				}
				// log.Debugf("TO ACK msg %v worker", envelope.Msg())
				err = envelope.Ack(false)
				unwrap(err, "Failed to ack message")
				// log.Debugf("Row processed: %v name: %v", row.Strings["title"], currentTask.Name())
			}
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

type SourceTask[O codec.Serializable[O]] struct {
	name string
}

func NewSourceTask[O codec.Serializable[O]](name string) task.Task[*model.Row, O] {
	return &SourceTask[O]{name}
}

func (t *SourceTask[O]) ProcessAndSend(envelope middleware.Envelope[*model.Row]) error {
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
	movies_metadata := NewSourceTask[*model.Row]("movies_metadata")
	credits := NewSourceTask[*model.Row]("credits")

	n_workers, err := strconv.Atoi(os.Getenv("N_WORKERS"))
	if err != nil {
		log.Fatalf("Failed to convert N_WORKERS to int: %s", err)
	}

	movies_metadata_clean := clean.NewCleanMovies(movies_metadata, []string{"filter_release_date_ge_2000_and_include_ar", "filter_one_production_country"}, uint(n_workers), uint(n_workers)) //, "map_sentiment_rate"

	var joiner_credits_subscribers []string
	for i := range n_workers {
		joiner_credits_subscribers = append(joiner_credits_subscribers, fmt.Sprintf("joiner_%d_credits", i))
	}

	credits_clean := clean.NewCleanCredits(credits, joiner_credits_subscribers, uint(1), uint(n_workers))

	filter_release_date_ge_2000_and_include_ar := filter.NewFilterReleaseDateGe2000AndIncludeAR(movies_metadata_clean.Name(), []string{"filter_release_date_l_2010_and_include_es", "joiner_credits", "joiner_ratings"}, uint(n_workers), uint(n_workers))
	filter_release_date_l_2010_and_include_es := filter.NewFilterReleaseDateL2010AndIncludeES(filter_release_date_ge_2000_and_include_ar.Name(), []string{"q1"}, 1, uint(n_workers))

	n_reducers_by_country_sum_budgets, err := strconv.Atoi(os.Getenv("N_REDUCERS_BY_COUNTRY_SUM_BUDGETS"))
	if err != nil {
		log.Fatalf("Failed to convert N_REDUCERS_BY_COUNTRY_SUM_BUDGETS to int: %s", err)
	}

	filter_one_production_country := filter.NewFilterProductionCountriesLen1(movies_metadata_clean.Name(), []string{"reduce_by_country_sum_budget"}, uint(n_reducers_by_country_sum_budgets), uint(n_workers))

	filter_avg_rate := filter.NewFilterAvgRate("reduce_by_sentiment", []string{"q5"}, 1, uint(n_workers))

	var joiner_ratings_subscribers []string
	for i := range n_workers {
		joiner_ratings_subscribers = append(joiner_ratings_subscribers, fmt.Sprintf("joiner_%d_ratings", i))
	}
	filter_avg_rating := filter.NewFilterAvgRating("reduce_by_movieId", joiner_ratings_subscribers, uint(1), uint(n_workers))

	return Worker{
		Tasks: []task.Task[*model.Row, *model.Row]{
			movies_metadata_clean,
			credits_clean,
			filter_release_date_ge_2000_and_include_ar,
			filter_release_date_l_2010_and_include_es,
			filter_one_production_country,
			filter_avg_rate,
			filter_avg_rating,
		},
	}
}
