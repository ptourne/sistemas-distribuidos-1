package main

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/client"
	"github.com/ptourne/sistemas-distribuidos-1/common/utils"
)

const HEADER_SIZE = 1
const MaxUDPMessageSize = 1024
const CHECK_INTERVAL = 3 * time.Second   // ToDo: ajustar
const TIMEOUT = 5 * time.Second          // ToDo: ajustar
const ELECTION_TIMEOUT = 3 * time.Second // ToDo: ajustar

type WorkerType int

const (
	WORKER WorkerType = iota
	CLIENT
	MONITOR
)

type WorkerStatus struct {
	LastSeen time.Time
	Type     WorkerType // ToDo: client
}

type Monitor struct {
	Workers         map[string]WorkerStatus
	MuWorkers       sync.Mutex
	Timeout         time.Duration
	port            string
	LeaderID        string
	Peers           map[string]string // id -> address (IP:PORT)
	inElection      bool
	MuInElection    sync.Mutex
	higherResponded bool
	MuAnswers       sync.Mutex
}

func NewMonitor(port, rawPeers string) *Monitor {
	peers := make(map[string]string)
	for _, peer := range strings.Split(rawPeers, ",") {
		parts := strings.Split(peer, ":")
		if len(parts) == 3 && parts[0] != MONITOR_ID {
			peers[parts[0]] = fmt.Sprintf("%s:%s", parts[1], parts[2])
		}
	}

	return &Monitor{
		Workers:         make(map[string]WorkerStatus),
		MuWorkers:       sync.Mutex{},
		Timeout:         TIMEOUT,
		port:            port,
		Peers:           peers,
		LeaderID:        "",
		inElection:      false,
		MuInElection:    sync.Mutex{},
		higherResponded: false,
		MuAnswers:       sync.Mutex{},
	}
}

func (m *Monitor) Start() {
	go m.startTCPServer()

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
	go m.checkLeaderAlive()
	go m.sendHeartbeat()

	m.checkWorkers(cli)
}

func (m *Monitor) sendHeartbeat() {
	name := m.name()
	peerAddrs := m.peerAddrs()
	log.Infof("Sending heartbeat to peers: %s", peerAddrs)
	utils.SendHeartbeat(name, peerAddrs, log)
}

func (m *Monitor) peerAddrs() string {
	addrs := make([]string, 0, len(m.Peers))
	for _, addr := range m.Peers {
		addrs = append(addrs, addr)
	}
	return strings.Join(addrs, ",")
}

func (m *Monitor) name() string {
	return "monitor" + MONITOR_ID
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

		workerType := WORKER
		if strings.Contains("client", id) {
			workerType = CLIENT
		}
		if strings.Contains("monitor", id) {
			workerType = MONITOR
		}

		m.MuWorkers.Lock()
		m.Workers[id] = WorkerStatus{LastSeen: time.Now(), Type: workerType}
		m.MuWorkers.Unlock()

	}
}

func (m *Monitor) checkWorkers(cli *client.Client) {
	for {
		time.Sleep(CHECK_INTERVAL)
		now := time.Now()

		m.MuWorkers.Lock()
		for id, status := range m.Workers {
			if now.Sub(status.LastSeen) > m.Timeout {
				if m.isLeader() {
					log.Infof("Worker %s not responding. Restarting...", id)
					m.restartContainer(cli, id)
				} else {
					log.Infof("Worker %s not responding.", id)
				}
				if status.Type == MONITOR {
					m.startElection()
				}
			}
		}
		m.MuWorkers.Unlock()
	}
}

func (m *Monitor) isLeader() bool {
	return m.LeaderID == MONITOR_ID
}

func unwrap(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
		panic(err)

	}
}

func sendMessage(addr, msg string) { // ToDo: short write
	var conn net.Conn
	var err error
	for range 5 {
		conn, err = net.Dial("tcp", addr)
		if err == nil {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	if err != nil || conn == nil {
		log.Warnf("Could not connect to %s: %v", addr, err)
		return
	}
	defer conn.Close()
	conn.Write([]byte(msg))
}

func (m *Monitor) startElection() {
	m.MuInElection.Lock()
	if m.inElection {
		log.Infof("Monitor %s already in election", MONITOR_ID)
		m.MuInElection.Unlock()
		return
	}
	m.inElection = true
	m.MuInElection.Unlock()
	log.Infof("Monitor %s starting election", MONITOR_ID)
	m.MuAnswers.Lock()
	m.higherResponded = false
	m.MuAnswers.Unlock()

	hasMaxId := true
	for id, addr := range m.Peers {
		if id > MONITOR_ID {
			go sendMessage(addr, fmt.Sprintf("ELECTION|%s", MONITOR_ID))
			hasMaxId = false
		}
	}
	if hasMaxId {
		log.Infof("I have the highest ID. Selecting myself as leader.")
		m.SelectMyselfAsLeader()
		return
	}

	time.Sleep(ELECTION_TIMEOUT)

	m.MuAnswers.Lock()
	if !m.higherResponded {
		m.SelectMyselfAsLeader()
	}
	m.MuAnswers.Unlock()
}

func (m *Monitor) SelectMyselfAsLeader() {
	m.LeaderID = MONITOR_ID
	m.announceCoordinator()
	m.MuInElection.Lock()
	m.inElection = false
	m.MuInElection.Unlock()
}

func (m *Monitor) announceCoordinator() {
	for _, addr := range m.Peers {
		go sendMessage(addr, fmt.Sprintf("COORDINATOR|%s", MONITOR_ID))
	}
}

func (m *Monitor) startTCPServer() {
	listener, err := net.Listen("tcp", ":"+m.port)
	if err != nil {
		log.Fatalf("TCP Listen error: %v", err)
	}
	log.Infof("TCP server on port %s", m.port)

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Warnf("Accept error: %v", err)
			continue
		}
		go m.handleConnection(conn)
	}
}

func (m *Monitor) handleConnection(conn net.Conn) {
	defer conn.Close()
	buffer := make([]byte, 256) // ToDo: short read
	n, _ := conn.Read(buffer)
	msg := string(buffer[:n])
	parts := strings.Split(msg, "|")

	switch parts[0] {
	case "ELECTION":
		senderID := parts[1]
		log.Infof("Received ELECTION from %s", senderID)
		go sendMessage(m.Peers[senderID], fmt.Sprintf("ANSWER|%s", MONITOR_ID))
		go m.startElection()
	case "ANSWER":
		log.Infof("Received ANSWER from %s", parts[1])
		m.MuAnswers.Lock()
		m.higherResponded = true
		m.MuAnswers.Unlock()
		// ToDo:Marcar que recibió respuesta y no se autoproclame
	case "COORDINATOR":
		m.LeaderID = parts[1]
		m.inElection = false
		log.Infof("New coordinator is %s", m.LeaderID)
	}
}

func (m *Monitor) checkLeaderAlive() {
	for {
		if m.LeaderID == "" {
			log.Infof("No leader elected yet. Starting election.")
			m.startElection()
			time.Sleep(CHECK_INTERVAL)
			continue
		}

		if m.LeaderID == MONITOR_ID {
			time.Sleep(CHECK_INTERVAL)

			continue
		}
		m.MuWorkers.Lock()
		status, ok := m.Workers["monitor"+m.LeaderID]
		m.MuWorkers.Unlock()

		if !ok || time.Since(status.LastSeen) > m.Timeout {
			log.Warnf("Leader %s not responding. Starting election.", m.LeaderID)
			go m.startElection()
		}
		time.Sleep(CHECK_INTERVAL)
	}
}
