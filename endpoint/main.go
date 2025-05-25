package main

import (
	"context"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/ptourne/sistemas-distribuidos-1/common/logger"
	"github.com/ptourne/sistemas-distribuidos-1/common/utils"
)

var log = logger.NewConsoleLogger("endpoint", logger.Debug)

func main() {
	name := os.Getenv("NAME")
	monitor_addrs := os.Getenv("MONITOR_ADDRESSES")
	wg := sync.WaitGroup{}
	wg.Add(1)
	finishChan := make(chan bool)

	endpoint, err := NewEndpoint()
	if err != nil {
		log.Errorf("error creando el endpoint: %v", err)
		return
	}
	go HandleSignals(endpoint, &wg, finishChan)
	ctxHeartbeat, cancelHearbeat := context.WithCancel(context.Background())
	go utils.SendHeartbeat(name, monitor_addrs, log, ctxHeartbeat)
	err = endpoint.Run()
	if err != nil {
		log.Errorf("error recibiendo archivos: %v", err)
	}
	if endpoint.Running {
		finishChan <- true
	}
	close(finishChan)
	wg.Wait()
	cancelHearbeat()
	log.Infof("endpoint finished")
	time.Sleep(1000 * time.Millisecond)

}

func HandleSignals(c *Endpoint, wg *sync.WaitGroup, finishChan chan bool) {
	defer wg.Done()
	sigChannel := make(chan os.Signal, 1) // espera las signals
	//crea un canal (chan) en Go que puede recibir valores del tipo os.Signal
	//el 1 en make(chan os.Signal, 1) significa que es un canal con buffer de tamaño 1
	signal.Notify(sigChannel, syscall.SIGTERM)
	//escuche la señal SIGTERM del sistema operativo.
	select {
	case <-finishChan:
		log.Infof("signal: finish")
	case <-sigChannel:
		log.Infof("signal: SIGTERM")
		//cuando SIGTERM ocurra, se enviará automáticamente al canal sigChannel
		//bloquea la ejecución hasta que el canal reciba la señal sigterm.
		c.StopEndpoint()
	}
}
