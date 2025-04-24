package main

import (
	"fmt"
	"os"
	"strconv"
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
		log.Infof("debug")
		envelope := <-inputChannels[0]
		currentTask := w.TasksBin

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
	// movies_metadata := NewSourceTask("movies_metadata")
	// credits := NewSourceTask("credits")
	ratings := NewSourceTask[[]byte]("ratings")
	// movies_metadata_clean := clean.NewCleanMovies(movies_metadata, []string{"filter_release_date_ge_2000_and_include_ar", "filter_one_production_country", "map_sentiment_rate"})
	n_worker, err := strconv.Atoi(os.Getenv("N_JOINERS")) // TODO: cambiar en el compose
	if err != nil {
		log.Fatalf("Failed to convert N_JOINERS to int: %s", err)
	}
	// var joiner_credits_subscribers []string
	var joiner_ratings_subscribers []string
	for i := range n_worker {
		_ = i
		// joiner_credits_subscribers = append(joiner_credits_subscribers, fmt.Sprintf("joiner_%d_credits", i+1))
		joiner_ratings_subscribers = append(joiner_ratings_subscribers, fmt.Sprintf("joiner_%d_ratings", i+1))
	}
	// credits_clean := clean.NewCleanCredits(credits, joiner_credits_subscribers)
	ratings_clean := clean.NewCleanRatings(ratings, joiner_ratings_subscribers)

	// filter_release_date_ge_2000_and_include_ar := filter.NewFilterReleaseDateGe2000AndIncludeAR(movies_metadata_clean.Name(), []string{"filter_release_date_l_2010_and_include_es", "joiner_credits","joiner_ratings"})
	// filter_release_date_l_2010_and_include_es := filter.NewFilterReleaseDateL2010AndIncludeES(filter_release_date_ge_2000_and_include_ar.Name(), []string{"q1"})
	// filter_one_production_country := filter.NewFilterProductionCountriesLen1(movies_metadata_clean.Name(), []string{"reduce_by_country_sum_budget"})
	// joiner_credits := joiner.NewJoinerCredits(filter_release_date_ge_2000_and_include_ar, credits_clean, []string{"reduce_by_actor"})
	// joiner_ratings := joiner.NewJoinerRatings(filter_release_date_ge_2000_and_include_ar, ratings_clean, []string{"q3"})

	// grpcAddress := os.Getenv("NLP_GRPC_ADDR")

	// map_nlp := filter.NewFilterSentimentAndRate(movies_metadata_clean.Name(), []string{"reduce_by_sentiment"}, grpcAddress)
	// filter_avg_rate := filter.NewFilterAvgRate("reduce_by_sentiment", []string{"q5"})

	return Worker{
		TasksBin: ratings_clean,
	}
}
