package main

import (
	"context"
	"fmt"
	"os"

	"github.com/ptourne/sistemas-distribuidos-1/common/logger"
	"github.com/ptourne/sistemas-distribuidos-1/common/utils"
)

func main() {

	name := os.Getenv("NAME")
	monitor_addrs := os.Getenv("MONITOR_ADDRESSES")
	var WORKER_ID = os.Getenv("WORKER_ID")
	var log = logger.NewConsoleLogger(fmt.Sprintf("worker_%s", WORKER_ID), logger.Info)
	worker := NewWorker()
	ctxHeartbeat, cancelHearbeat := context.WithCancel(context.Background())
	go utils.SendHeartbeat(name, monitor_addrs, log, ctxHeartbeat)
	defer cancelHearbeat()
	worker.Run()
	
	log.Infof("worker finished")
}
