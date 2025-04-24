package main

import (
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/ptourne/sistemas-distribuidos-1/common/logger"
)

var log = logger.NewConsoleLogger("client", logger.Debug)

type Client struct {
	conn    net.Conn
	Running bool
}

func main() {
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
	go HandleSignals(client, &wg, finishChan)
	err = client.Run()
	if err != nil && client.Running {
		log.Errorf("error sending files: %v", err)
	}
	if client.Running {
		finishChan <- true
	}
	close(finishChan)
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
