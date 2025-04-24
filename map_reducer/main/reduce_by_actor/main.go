package main

import (
	"fmt"
	"os"

	"github.com/ptourne/sistemas-distribuidos-1/common/logger"
	map_reducer_sentiment "github.com/ptourne/sistemas-distribuidos-1/map_reducer/generics/map_reducer_by_actor"
)

var WORKER_ID = os.Getenv("WORKER_ID")
var log = logger.NewConsoleLogger(fmt.Sprintf("reduce_by_actor_%s", WORKER_ID), logger.Info)

func main() {
	mapReducer, err := map_reducer_sentiment.NewMapReducerByActor("reduce_by_actor", "joiner_credits", 2, []string{"reduce_top_10_by_actor"})
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
