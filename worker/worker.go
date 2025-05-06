package main

import (
	"fmt"
	"os"
	"reflect"
	"slices"
	"strings"

	"github.com/ptourne/sistemas-distribuidos-1/common/logger"
	"github.com/ptourne/sistemas-distribuidos-1/common/model"
	"github.com/ptourne/sistemas-distribuidos-1/middleware/codec"
	"github.com/ptourne/sistemas-distribuidos-1/middleware/middleware"
	"github.com/ptourne/sistemas-distribuidos-1/middleware/middleware/rabbitmq"

	//"github.com/ptourne/sistemas-distribuidos-1/worker/joiner"

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

	for {
		if len(cases) == 0 {
			log.Infof("All channels closed")
			break
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

			result := currentTask.ProcessAndSend(envelope)
			if result != nil {
				log.Errorf("Failed to process fileChunk: %v by task: %v", envelope, currentTask.Name())
				continue
			}
			// log.Debugf("TO ACK msg %v worker", envelope.Msg())
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
	// credits := NewSourceTask[*model.Row]("credits")
	movies_metadata_clean := clean.NewCleanMovies(movies_metadata, []string{"filter_release_date_ge_2000_and_include_ar", "filter_one_production_country", "map_sentiment_rate"})

	// n_worker, err := strconv.Atoi(os.Getenv("N_JOINERS")) // TODO: cambiar en el compose
	// if err != nil {
	// 	log.Fatalf("Failed to convert N_JOINERS to int: %s", err)
	// }

	// var joiner_credits_subscribers []string
	// for i := range n_worker {
	// 	joiner_credits_subscribers = append(joiner_credits_subscribers, fmt.Sprintf("joiner_%d_credits", i+1))
	// }

	// credits_clean := clean.NewCleanCredits(credits, joiner_credits_subscribers)

	filter_release_date_ge_2000_and_include_ar := filter.NewFilterReleaseDateGe2000AndIncludeAR(movies_metadata_clean.Name(), []string{"q1"}) //"filter_release_date_l_2010_and_include_es" "joiner_credits", "joiner_ratings"
	filter_release_date_l_2010_and_include_es := filter.NewFilterReleaseDateL2010AndIncludeES(filter_release_date_ge_2000_and_include_ar.Name(), []string{"q1"})
	filter_one_production_country := filter.NewFilterProductionCountriesLen1(movies_metadata_clean.Name(), []string{"reduce_by_country_sum_budget"})
	// joiner_credits := joiner.NewJoinerCredits(filter_release_date_ge_2000_and_include_ar, credits_clean, []string{"reduce_by_actor"})

	// grpcAddress := os.Getenv("NLP_GRPC_ADDR")

	// map_nlp := filter.NewFilterSentimentAndRate(movies_metadata_clean.Name(), []string{"reduce_by_sentiment"}, grpcAddress)
	// filter_avg_rate := filter.NewFilterAvgRate("reduce_by_sentiment", []string{"q5"})

	// filter_avg_rating := filter.NewFilterAvgRating("reduce_by_movieId", joiner_ratings_subscribers)

	return Worker{
		Tasks: []task.Task[*model.Row, *model.Row]{
			movies_metadata_clean,
			// credits_clean,
			filter_release_date_ge_2000_and_include_ar,
			filter_release_date_l_2010_and_include_es,
			filter_one_production_country,
			// joiner_credits,
			// map_nlp,
			// filter_avg_rate,
			// filter_avg_rating,
		},
	}
}
