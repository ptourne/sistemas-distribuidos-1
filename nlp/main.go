package main

import (
	"context"
	"os"

	"github.com/ptourne/sistemas-distribuidos-1/common/utils"
)

func main() {
	name := os.Getenv("NAME")
	monitor_addrs := os.Getenv("MONITOR_ADDRESSES")

	worker := NewWorker()
	ctxHeartbeat, cancelHearbeat := context.WithCancel(context.Background())
	defer cancelHearbeat()
	go utils.SendHeartbeat(name, monitor_addrs, log, ctxHeartbeat)

	worker.Run()
	log.Infof("worker finished")
}
