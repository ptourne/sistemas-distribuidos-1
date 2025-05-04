package main

import (
	"fmt"
	"os"

	"github.com/ptourne/sistemas-distribuidos-1/common/logger"
	map_reducer_sentiment "github.com/ptourne/sistemas-distribuidos-1/map_reducer/generics/map_reducer_by_sentiment"
	"github.com/ptourne/sistemas-distribuidos-1/middleware/middleware/rabbitmq"
)

var WORKER_ID = os.Getenv("WORKER_ID")
var log = logger.NewConsoleLogger(fmt.Sprintf("reduce_by_sentiment_%s", WORKER_ID), logger.Info)

func main() {
	connector, err := rabbitmq.Connector()
	if err != nil {
		log.Errorf("failed to create connector: %w", err)
		return
	}
	mapReducer, err := map_reducer_sentiment.NewMapReducerBySentiment(
		connector,
		"reduce_by_sentiment",
		"map_sentiment_rate",
		2,
		[]string{"filter_avg_rate"},
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
