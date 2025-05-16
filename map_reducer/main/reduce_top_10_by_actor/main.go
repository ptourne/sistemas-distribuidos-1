package main

import (
	"context"
	"fmt"
	"os"
	"strconv"

	"github.com/ptourne/sistemas-distribuidos-1/common/logger"
	top_map_reduce "github.com/ptourne/sistemas-distribuidos-1/map_reducer/generics/top_map_reduce_actor"
	"github.com/ptourne/sistemas-distribuidos-1/middleware/middleware/rabbitmq"
)

var WORKER_ID = os.Getenv("WORKER_ID")
var log = logger.NewConsoleLogger(fmt.Sprintf("reduce_top_5_by_budget_%s", WORKER_ID), logger.Info)

func main() {
	connector, err := rabbitmq.Connector("reduce-top-10-by-actor")
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
	mapReducer, err := top_map_reduce.NewTopMapReducerActor(
		connector,
		"reduce_top_10_by_actor",
		"reduce_by_actor",
		10,
		2,
		[]string{"q4"},
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
