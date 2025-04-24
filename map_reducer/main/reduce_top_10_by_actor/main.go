package main

import (
	"fmt"
	"os"

	"github.com/ptourne/sistemas-distribuidos-1/common/logger"
	top_map_reduce "github.com/ptourne/sistemas-distribuidos-1/map_reducer/generics/top_map_reduce_actor"
)

var WORKER_ID = os.Getenv("WORKER_ID")
var log = logger.NewConsoleLogger(fmt.Sprintf("reduce_top_5_by_budget_%s", WORKER_ID), logger.Info)

func main() {
	mapReducer, err := top_map_reduce.NewTopMapReducerActor("reduce_top_10_by_actor", "reduce_by_actor", 10, 2, []string{"q4"})
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
