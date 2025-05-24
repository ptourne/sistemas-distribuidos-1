package main

import (
	"fmt"
	"os"

	"github.com/ptourne/sistemas-distribuidos-1/common/logger"
)

var WORKER_ID = os.Getenv("WORKER_ID")
var log = logger.NewConsoleLogger(fmt.Sprintf("monitor_%s", WORKER_ID), logger.Info)

func main() {

	port := os.Getenv("PORT")

	monitor := NewMonitor(port)
	monitor.Start()
}
