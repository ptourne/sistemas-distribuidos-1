package main

import (
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/ptourne/sistemas-distribuidos-1/common/logger"
	"github.com/ptourne/sistemas-distribuidos-1/common/utils"
)

var log = logger.NewConsoleLogger("client", logger.Debug)

type Client struct {
	conn    net.Conn
	Running bool
}

func main() {
	name := os.Getenv("CLIENT_NAME")
	monitor_addrs := os.Getenv("MONITOR_ADDRESSES")
	var SERVER_PORT = os.Getenv("SERVER_PORT")
	conn, err := net.Dial("tcp", SERVER_PORT)
	if err != nil {
		panic(err)
	}
	client := &Client{
		Running: true,
		conn:    conn,
	}
	wg := sync.WaitGroup{}
	wg.Add(1)
	finishChan := make(chan bool)
	heartbeatStop := make(chan bool)
	go HandleSignals(client, &wg, finishChan)
	go utils.SendStoppableHeartbeat(name, monitor_addrs, log, heartbeatStop)
	err = client.Run()
	if err != nil && client.Running {
		log.Errorf("error sending files: %v", err)
	}
	if client.Running {
		finishChan <- true
	}
	close(finishChan)
	wg.Add(1)
	heartbeatStop <- true
	close(heartbeatStop)
	go func() {
		utils.SendExit(name, monitor_addrs, log)
		wg.Done()
	}()
	wg.Wait()
	log.Infof("client finished")
	time.Sleep(1000 * time.Millisecond)

}

func HandleSignals(c *Client, wg *sync.WaitGroup, finishChan chan bool) {
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
		c.StopClient()
	}
}
