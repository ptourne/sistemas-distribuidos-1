package main

import (
	"fmt"
	"os"

	"github.com/ptourne/sistemas-distribuidos-1/common/logger"
	"github.com/ptourne/sistemas-distribuidos-1/map_reducer/generics/map_reducer_sum"
)

var WORKER_ID = os.Getenv("WORKER_ID")
var log = logger.NewConsoleLogger(fmt.Sprintf("reduce_by_country_sum_budget_%s", WORKER_ID), logger.Info)

func main() {
	mapReducer, err := map_reducer_sum.NewMapReducerSum("reduce_by_country_sum_budget", "filter_one_production_country", 2)
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
