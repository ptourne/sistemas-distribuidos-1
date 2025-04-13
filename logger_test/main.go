package main

import (
	"fmt"

	"github.com/ptourne/sistemas-distribuidos-1/common/logger"
)

func main() {
	log := logger.NewConsoleLogger("test", logger.Debug)
	log.Debugf("This is a %s message", "debug")
	log.Infof("This is an %s message", "info")
	log.Warnf("This is a %s message", "warning")
	log.Errorf("This is an %s message", "error")
	log.Fatalf("This is a %s message", "fatal")
	for i := 0; i < 10; i++ {
		otherLog := logger.NewConsoleLogger(fmt.Sprintf("actor %d", i), logger.Debug)
		otherLog.Infof("This is message from actor %d", i)
	}
}
