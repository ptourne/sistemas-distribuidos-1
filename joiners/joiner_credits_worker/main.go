package main

import (
	"github.com/ptourne/sistemas-distribuidos-1/middleware/middleware/rabbitmq"

	"github.com/ptourne/sistemas-distribuidos-1/common/model"
	"github.com/ptourne/sistemas-distribuidos-1/joiners/joiner_credits_worker/credits"
)

func main() {
	worker := credits.NewWorker([]string{"reduce_by_actor"})
	connector, err := rabbitmq.Connector()
	if err != nil {
		credits.Log.Fatalf("Failed to connect to middleware: %s", err)
	}
	middlewareConnection := rabbitmq.NewMiddleware[*model.Row](connector)
	defer middlewareConnection.Close()

	worker.Run(middlewareConnection)
}
