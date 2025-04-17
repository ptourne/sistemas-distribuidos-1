package filter

import (
	"fmt"
	"os"

	"github.com/ptourne/sistemas-distribuidos-1/common/logger"
)

var WORKER_ID = os.Getenv("WORKER_ID")
var log = logger.NewConsoleLogger(fmt.Sprintf("worker_%s", WORKER_ID), logger.Debug)
