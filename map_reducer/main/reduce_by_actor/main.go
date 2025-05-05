package main

import (
	"context"
	"fmt"
	"os"
	"strconv"

	"github.com/ptourne/sistemas-distribuidos-1/common/logger"
	map_reducer_sentiment "github.com/ptourne/sistemas-distribuidos-1/map_reducer/generics/map_reducer_by_actor"
	"github.com/ptourne/sistemas-distribuidos-1/middleware/middleware/rabbitmq"
)

var WORKER_ID = os.Getenv("WORKER_ID")
var log = logger.NewConsoleLogger(fmt.Sprintf("reduce_by_actor_%s", WORKER_ID), logger.Info)

func main() {
	connector, err := rabbitmq.Connector()
	if err != nil {
		log.Errorf("failed to create connector: %s", err)
		return
	}
	id := os.Getenv("WORKER_ID")
	count_str := os.Getenv("WORKER_COUNT")
	count, err := strconv.Atoi(count_str)
	if err != nil {
		log.Errorf("failed to parse worker count: %s", err)
		return
	}

	mapReducer, err := map_reducer_sentiment.NewMapReducerByActor(
		connector,
		"reduce_by_actor",
		"joiner_credits",
		2,
		[]string{"reduce_top_10_by_actor"},
		id,
		uint(count),
	)
	if err != nil {
		log.Errorf("error creating maperducer: %s", err)
		return
	}
	err = mapReducer.Run(context.Background()) // TODO: use context to handle sigterm
	if err != nil {
		log.Errorf("error running map reducer: %s", err)
		return
	}
	log.Infof("map reducer finished")
}
