package main

import (
	"context"
	"net"
	"sync"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/client"
)

const HEADER_SIZE = 1
const MaxUDPMessageSize = 1024
const CHECK_INTERVAL = 5 * time.Second // ToDo: ajustar
const TIMEOUT = 15 * time.Second       // ToDo: ajustar

type WorkerStatus struct {
	LastSeen time.Time
	Type     string
}

type Monitor struct {
	Workers map[string]WorkerStatus
	Mu      sync.Mutex
	Timeout time.Duration
	port    string
}

func NewMonitor(port string) *Monitor {
	return &Monitor{
		Workers: make(map[string]WorkerStatus),
		Timeout: TIMEOUT,
		port:    port,
	}
}
func (m *Monitor) Start() {

	addr, err := net.ResolveUDPAddr("udp", ":"+m.port)
	unwrap(err, "Failed to resolve UDP address")
	conn, err := net.ListenUDP("udp", addr)
	unwrap(err, "Failed to listen on UDP")
	defer conn.Close()

	cli, err := client.NewClientWithOpts(client.WithHost("unix:///var/run/docker.sock"), client.WithAPIVersionNegotiation())
	if err != nil {
		log.Fatalf("Error creando cliente Docker: %v", err)
	}

	log.Infof("Monitor listening on %s", addr.String())
	go m.listenHeartbeats(conn)

	m.checkWorkers(cli)

}

func (m *Monitor) restartContainer(cli *client.Client, containerName string) {
	ctx := context.Background()
	stopOptions := container.StopOptions{
		Timeout: nil,
		Signal:  "SIGTERM",
	}
	err := cli.ContainerRestart(ctx, containerName, stopOptions)
	if err != nil {
		log.Errorf("Failed to restart %s: %v", containerName, err)
	} else {
		log.Infof("Container %s restarted", containerName)
	}
}

func (m *Monitor) listenHeartbeats(conn *net.UDPConn) {
	buffer := make([]byte, MaxUDPMessageSize)
	for {
		n, _, err := conn.ReadFromUDP(buffer)
		if err != nil {
			log.Infof("Error reading UDP: %v", err)
			continue
		}

		if n < HEADER_SIZE {
			log.Errorf("Received incomplete message: got %d bytes, expected at least %d", n, HEADER_SIZE)
			continue
		}

		msgLen := int(buffer[0])
		if n-HEADER_SIZE != msgLen {
			log.Errorf("Received incomplete message: got %d bytes, expected %d", n-HEADER_SIZE, msgLen)
			continue
		}

		id := string(buffer[HEADER_SIZE : HEADER_SIZE+msgLen])

		m.Mu.Lock()
		m.Workers[id] = WorkerStatus{LastSeen: time.Now()}
		m.Mu.Unlock()

	}
}

func (m *Monitor) checkWorkers(cli *client.Client) {
	for {
		time.Sleep(CHECK_INTERVAL)
		now := time.Now()

		m.Mu.Lock()
		for id, status := range m.Workers {
			if now.Sub(status.LastSeen) > m.Timeout {
				log.Infof("Worker %s not responding. Restarting...", id)
				m.restartContainer(cli, id)
			}
		}
		m.Mu.Unlock()
	}
}

func unwrap(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
		panic(err)

	}
}
