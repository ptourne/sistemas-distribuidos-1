package main

import (
	"fmt"
	"os"

	"github.com/ptourne/sistemas-distribuidos-1/common/logger"
	"github.com/ptourne/sistemas-distribuidos-1/common/utils"
)

func main() {

	name := os.Getenv("WORKER_NAME")
	monitor_addrs := os.Getenv("MONITOR_ADDRESSES")
	var WORKER_ID = os.Getenv("WORKER_ID")
	var log = logger.NewConsoleLogger(fmt.Sprintf("worker_%s", WORKER_ID), logger.Info)
	worker := NewWorker()

	go utils.SendHeartbeat(name, monitor_addrs, log)

	worker.Run()
}
