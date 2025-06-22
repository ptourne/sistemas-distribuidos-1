package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/ptourne/sistemas-distribuidos-1/common/logger"
	"github.com/ptourne/sistemas-distribuidos-1/common/utils"
	"github.com/ptourne/sistemas-distribuidos-1/map_reducer/generics/map_reducer_sum"
	"github.com/ptourne/sistemas-distribuidos-1/middleware/middleware/rabbitmq"
)

var WORKER_ID = os.Getenv("WORKER_ID")
var log = logger.NewConsoleLogger(fmt.Sprintf("reduce_by_country_sum_budget_%s", WORKER_ID), logger.Info)

func main() {
	name := os.Getenv("NAME")
	monitor_addrs := os.Getenv("MONITOR_ADDRESSES")
	logDir := os.Getenv("LOG_DIR")
	maxLogSizeStr := os.Getenv("MAX_LOG_SIZE")
	maxLogSize, err := strconv.Atoi(maxLogSizeStr)
	if err != nil {
		log.Errorf("failed to max log size: %s", err)
		return
	}

	connector, err := rabbitmq.Connector()
	if err != nil {
		log.Errorf("failed to create connector: %s", err)
		return
	}
	id := os.Getenv("WORKER_ID")
	count_str := os.Getenv("WORKER_COUNT")
	count, err := strconv.Atoi(count_str)
	if err != nil {
		log.Errorf("failed to parse worker count: %s", err)
		return
	}
	count_output_str := os.Getenv("WORKER_OUTPUT_COUNT")
	count_output, err := strconv.Atoi(count_output_str)
	if err != nil {
		log.Errorf("failed to parse worker count: %s", err)
		return
	}
	mapReducer, err := map_reducer_sum.NewMapReducerSum(
		connector,
		"reduce_by_country_sum_budget",
		"filter_one_production_country",
		2,
		[]string{"reduce_top_5_by_budget"},
		id,
		uint(count),
		uint(count_output),
		logDir,
		uint64(maxLogSize),
	)
	if err != nil {
		log.Errorf("error creating maperducer: %s", err)
		return
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctxHeartbeat, cancelHearbeat := context.WithCancel(context.Background())
	defer cancelHearbeat()
	go utils.SendHeartbeat(name, monitor_addrs, log, ctxHeartbeat)
	err = mapReducer.Run(ctx) // TODO: use context to handle sigterm
	if err != nil {
		log.Errorf("error running map reducer: %s", err)
		return
	}
	log.Infof("map reducer finished")
}
