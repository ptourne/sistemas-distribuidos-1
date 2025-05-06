package main

import (
	"fmt"
	"os"

	"github.com/ptourne/sistemas-distribuidos-1/common/logger"
	"github.com/ptourne/sistemas-distribuidos-1/joiners_ratings_workers/joiner"
	"github.com/ptourne/sistemas-distribuidos-1/middleware/middleware/rabbitmq"

	"github.com/ptourne/sistemas-distribuidos-1/common/model"
)

func GetEnv(key, defaultValue string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return defaultValue
}

var WORKER_ID = GetEnv("WORKER_ID", "1")

func main() {
	workerLogger := logger.NewConsoleLogger(fmt.Sprintf("joiner_%s", WORKER_ID), logger.Info)
	worker := joiner.NewCreditsWorker([]string{"reduce_by_actor"}, WORKER_ID, workerLogger)
	connector, err := rabbitmq.Connector()
	if err != nil {
		workerLogger.Fatalf("Failed to connect to middleware: %s", err)
	}
	middlewareLogger := logger.NewConsoleLogger(fmt.Sprintf("middleware_%s", WORKER_ID), logger.Info)
	middlewareConnection := rabbitmq.NewMiddleware[*model.Row](connector, middlewareLogger)
	defer middlewareConnection.Close()

	worker.Run(middlewareConnection)
}
