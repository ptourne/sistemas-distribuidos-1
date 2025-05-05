package main

import (
	"fmt"

	"github.com/ptourne/sistemas-distribuidos-1/common/logger"
	"github.com/ptourne/sistemas-distribuidos-1/joiners/joiner"
	"github.com/ptourne/sistemas-distribuidos-1/joiners/joiner_ratings_worker/ratings"
	"github.com/ptourne/sistemas-distribuidos-1/middleware/middleware/rabbitmq"

	"github.com/ptourne/sistemas-distribuidos-1/common/model"
)

func main() {

	worker := joiner.NewRatingsWorker([]string{"reduce_top_bottom_avg_rating"})
	connector, err := rabbitmq.Connector()
	if err != nil {
		ratings.Log.Fatalf("Failed to connect to middleware: %s", err)
	}
	id := ratings.WORKER_ID
	middlewareLogger := logger.NewConsoleLogger(fmt.Sprintf("middleware_%s", id), logger.Debug)
	middlewareConnection := rabbitmq.NewMiddleware[*model.Row](connector, middlewareLogger)
	defer middlewareConnection.Close()

	worker.Run(middlewareConnection)

}
