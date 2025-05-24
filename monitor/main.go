package main

import (
	"fmt"
	"os"

	"github.com/ptourne/sistemas-distribuidos-1/common/logger"
)

var MONITOR_ID = os.Getenv("MONITOR_ID")
var log = logger.NewConsoleLogger(fmt.Sprintf("monitor_%s", MONITOR_ID), logger.Info)

func main() {

	port := os.Getenv("PORT")
	rawPeers := os.Getenv("PEERS")

	monitor := NewMonitor(port, rawPeers)
	monitor.Start()
}
