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
	map_reducer_movieId "github.com/ptourne/sistemas-distribuidos-1/map_reducer/generics/map_reducer_by_movieId"
	"github.com/ptourne/sistemas-distribuidos-1/middleware/middleware/rabbitmq"
)

var WORKER_ID = os.Getenv("WORKER_ID")
var log = logger.NewConsoleLogger(fmt.Sprintf("reduce_by_movieId_%s", WORKER_ID), logger.Info)

func main() {
	name := os.Getenv("NAME")
	monitor_addrs := os.Getenv("MONITOR_ADDRESSES")
	logDir := os.Getenv("LOG_DIR")
	connector, err := rabbitmq.Connector()
	if err != nil {
		log.Errorf("failed to create connector: %s", err)
		return
	}
	id := os.Getenv("WORKER_ID")
	count_str := os.Getenv("WORKER_COUNT")
	count_shard, err := strconv.Atoi(count_str)
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
	rk, err := strconv.Atoi(WORKER_ID)
	if err != nil {
		log.Errorf("error converting WORKER_ID to int: %s", err)
		return
	}

	rkString := fmt.Sprintf("%d", rk-1)

	mapReducer, err := map_reducer_movieId.NewMapReducerByMovieId(
		connector,
		"reduce_by_movieId",
		"ratings",
		rkString,
		[]string{"0", "1", "2", "3", "4", "5", "6", "7", "8", "9"},
		2,
		[]string{"filter_avg_rating"},
		id,
		uint(count_shard),
		uint(count_output),
		logDir,
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
