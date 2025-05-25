package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/ptourne/sistemas-distribuidos-1/common/logger"
)

var MONITOR_ID = os.Getenv("MONITOR_ID")
var log = logger.NewConsoleLogger(fmt.Sprintf("monitor_%s", MONITOR_ID), logger.Info)

func main() {

	port := os.Getenv("PORT")
	rawPeers := os.Getenv("PEERS")
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	monitor := NewMonitor(port, rawPeers)
	monitor.Start(ctx)
}
