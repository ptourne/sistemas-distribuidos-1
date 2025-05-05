package main

import (
	"github.com/ptourne/sistemas-distribuidos-1/joiners_ratings_workers/joiner_ratings_worker/ratings"
	"github.com/ptourne/sistemas-distribuidos-1/middleware/middleware/rabbitmq"

	"github.com/ptourne/sistemas-distribuidos-1/common/model"
)

func main() {

	worker := ratings.NewWorker([]string{"reduce_top_bottom_avg_rating"})
	connector, err := rabbitmq.Connector()
	if err != nil {
		ratings.Log.Fatalf("Failed to connect to middleware: %s", err)
	}
	middlewareConnection := rabbitmq.NewMiddleware[*model.Row](connector)
	defer middlewareConnection.Close()

	worker.Run(middlewareConnection)
}
