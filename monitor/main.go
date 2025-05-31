package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
)

var MONITOR_ID = os.Getenv("MONITOR_ID")

func main() {

	port := os.Getenv("PORT")
	rawPeers := os.Getenv("PEERS")
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	monitor := NewMonitor(MONITOR_ID, port, rawPeers)
	monitor.Start(ctx)
}
