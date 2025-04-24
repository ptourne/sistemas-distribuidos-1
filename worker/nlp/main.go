package main

import (
	"fmt"
	"os"

	"github.com/ptourne/sistemas-distribuidos-1/common/logger"
	"github.com/ptourne/sistemas-distribuidos-1/worker/filter"
)

var WORKER_ID = os.Getenv("WORKER_ID")
var log = logger.NewConsoleLogger(fmt.Sprintf("nlp_worker%s", WORKER_ID), logger.Info)

func main() {
	grpcAddress := os.Getenv("NLP_GRPC_ADDR")

	map_nlp := filter.NewFilterSentimentAndRate("clean_movies", []string{"reduce_by_sentiment"}, grpcAddress)

	err := map_nlp.Run()
	if err != nil {
		log.Errorf("error running map nlp: %s", err)
		return
	}
	log.Infof("map nlp finished")
}
