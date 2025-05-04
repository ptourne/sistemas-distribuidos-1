package main

import (
	"fmt"
	"os"

	"github.com/ptourne/sistemas-distribuidos-1/common/logger"
	"github.com/ptourne/sistemas-distribuidos-1/map_reducer/generics/top_map_reduce"
	"github.com/ptourne/sistemas-distribuidos-1/middleware/middleware/rabbitmq"
)

var WORKER_ID = os.Getenv("WORKER_ID")
var log = logger.NewConsoleLogger(fmt.Sprintf("reduce_top_5_by_budget_%s", WORKER_ID), logger.Info)

func main() {
	connector, err := rabbitmq.Connector()
	if err != nil {
		log.Errorf("failed to create connector: %w", err)
		return
	}
	mapReducer, err := top_map_reduce.NewTopMapReducer(
		connector,
		"reduce_top_5_by_budget",
		"reduce_by_country_sum_budget",
		5,
		2,
		[]string{"q2"},
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
