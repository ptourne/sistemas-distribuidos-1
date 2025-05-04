package joiner

import (
	"os"
)

var WORKER_ID = os.Getenv("WORKER_ID")

// var log = logger.NewConsoleLogger(fmt.Sprintf("worker_%s", WORKER_ID), logger.Info)
