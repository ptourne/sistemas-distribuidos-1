package main

import (
	"context"
	"fmt"
	"os"

	"github.com/ptourne/sistemas-distribuidos-1/common/logger"
	"github.com/ptourne/sistemas-distribuidos-1/common/utils"
	"github.com/ptourne/sistemas-distribuidos-1/joiners/joiner"
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
	name := os.Getenv("NAME")
	monitor_addrs := os.Getenv("MONITOR_ADDRESSES")
	workerLogger := logger.NewConsoleLogger(fmt.Sprintf("joiner_%s", WORKER_ID), logger.Debug)
	worker := joiner.NewRatingsWorker([]string{"reduce_top_bottom_avg_rating"}, WORKER_ID, workerLogger)
	connector, err := rabbitmq.Connector()
	if err != nil {
		workerLogger.Fatalf("Failed to connect to middleware: %s", err)
	}
	middlewareLogger := logger.NewConsoleLogger(fmt.Sprintf("middleware_%s", WORKER_ID), logger.Debug)
	middlewareConnection := rabbitmq.NewMiddleware[*model.Row](connector, middlewareLogger)
	defer middlewareConnection.Close()

	ctxHeartbeat, cancelHearbeat := context.WithCancel(context.Background())
	defer cancelHearbeat()
	go utils.SendHeartbeat(name, monitor_addrs, workerLogger, ctxHeartbeat)
	worker.Run(middlewareConnection)
	workerLogger.Infof("worker finished")

}
