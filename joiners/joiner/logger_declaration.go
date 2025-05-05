package joiner

import (
	"fmt"
	"os"

	"github.com/ptourne/sistemas-distribuidos-1/common/logger"
)

func GetEnv(key, defaultValue string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return defaultValue
}

var WORKER_ID = GetEnv("WORKER_ID", "1")
var log = logger.NewConsoleLogger(fmt.Sprintf("joiner_%s", WORKER_ID), logger.Info)
