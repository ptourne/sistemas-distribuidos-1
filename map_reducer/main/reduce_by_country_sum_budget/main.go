package main

import (
	"fmt"
	"os"
	"strconv"

	"github.com/ptourne/sistemas-distribuidos-1/common/logger"
	"github.com/ptourne/sistemas-distribuidos-1/map_reducer/generics/map_reducer_sum"
	"github.com/ptourne/sistemas-distribuidos-1/middleware/middleware/rabbitmq"
)

var WORKER_ID = os.Getenv("WORKER_ID")
var log = logger.NewConsoleLogger(fmt.Sprintf("reduce_by_country_sum_budget_%s", WORKER_ID), logger.Info)

func main() {
	connector, err := rabbitmq.Connector()
	if err != nil {
		log.Errorf("failed to create connector: %w", err)
		return
	}
	id := os.Getenv("WORKER_ID")
	count_str := os.Getenv("WORKER_COUNT")
	count, err := strconv.Atoi(count_str)
	if err != nil {
		log.Errorf("failed to parse worker count: %w", err)
		return
	}
	mapReducer, err := map_reducer_sum.NewMapReducerSum(
		connector,
		"reduce_by_country_sum_budget",
		"filter_one_production_country",
		2,
		[]string{"reduce_top_5_by_budget"},
		id,
		uint(count),
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
