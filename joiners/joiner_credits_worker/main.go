package main

import (
	"fmt"

	"github.com/ptourne/sistemas-distribuidos-1/common/logger"
	"github.com/ptourne/sistemas-distribuidos-1/joiners_ratings_workers/joiner"
	"github.com/ptourne/sistemas-distribuidos-1/middleware/middleware/rabbitmq"

	"github.com/ptourne/sistemas-distribuidos-1/common/model"
	"github.com/ptourne/sistemas-distribuidos-1/joiners/joiner_credits_worker/credits"
)

func main() {
	worker := joiner.NewCreditsWorker([]string{"reduce_by_actor"})
	connector, err := rabbitmq.Connector()
	if err != nil {
		credits.Log.Fatalf("Failed to connect to middleware: %s", err)
	}
	id := credits.WORKER_ID
	middlewareLogger := logger.NewConsoleLogger(fmt.Sprintf("middleware_%s", id), logger.Info)
	middlewareConnection := rabbitmq.NewMiddleware[*model.Row](connector, middlewareLogger)
	defer middlewareConnection.Close()

	worker.Run(middlewareConnection)
}
