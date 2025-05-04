package main

import (
	"fmt"
	"os"
	"strconv"

	"github.com/ptourne/sistemas-distribuidos-1/common/logger"
	map_reducer_movieId "github.com/ptourne/sistemas-distribuidos-1/map_reducer/generics/map_reducer_by_movieId"
	"github.com/ptourne/sistemas-distribuidos-1/middleware/middleware/rabbitmq"
)

var WORKER_ID = os.Getenv("WORKER_ID")
var log = logger.NewConsoleLogger(fmt.Sprintf("reduce_by_movieId_%s", WORKER_ID), logger.Info)

func main() {
	connector, err := rabbitmq.Connector()
	if err != nil {
		log.Errorf("failed to create connector: %w", err)
		return
	}
	rk, err := strconv.Atoi(WORKER_ID)
	if err != nil {
		log.Errorf("error converting WORKER_ID to int: %s", err)
		return
	}

	rkString := fmt.Sprintf("%d", rk-1)

	mapReducer, err := map_reducer_movieId.NewMapReducerByMovieId(
		connector,
		"reduce_by_movieId",
		"clean_ratings",
		[]string{rkString},
		2,
		[]string{"filter_avg_rating"},
	)
	if err != nil {
		log.Errorf("error creating maperducer: %s", err)
		return
	}
	err = mapReducer.Run()
	if err != nil {
		log.Errorf("error running map reducer: %s", err)
		return
	}
	log.Infof("map reducer finished")
}
