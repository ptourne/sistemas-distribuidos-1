package main

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/client"
	"github.com/ptourne/sistemas-distribuidos-1/common/utils"
)

const HEADER_SIZE = 1
const MaxUDPMessageSize = 1024
const CHECK_INTERVAL = 3 * time.Second    // ToDo: ajustar
const TIMEOUT = 5 * time.Second           // ToDo: ajustar
const STARTING_TIMEOUT = 1 * time.Minute  // ToDo: ajustar
const ELECTION_TIMEOUT = 10 * time.Second // ToDo: ajustar

type WorkerType int
type Status int

const (
	WORKER WorkerType = iota
	CLIENT
	MONITOR
	SENTIMENT_SERVER
)

const (
	STARTING Status = iota
	RUNNING
	EXITED
)

type WorkerStatus struct {
	LastSeen time.Time
	Type     WorkerType // ToDo: client
	Status   Status
}

type Monitor struct {
	Workers      map[string]WorkerStatus
	MuWorkers    sync.Mutex
	Timeout      time.Duration
	port         string
	LeaderID     string
	MuLeader     sync.Mutex
	Peers        map[string]string // id -> address (IP:PORT)
	inElection   bool
	MuInElection sync.Mutex
	answerChan   chan bool
	MuAnswerChan sync.Mutex
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
		Workers:      make(map[string]WorkerStatus),
		MuWorkers:    sync.Mutex{},
		Timeout:      TIMEOUT,
		port:         port,
		Peers:        peers,
		LeaderID:     "",
		MuLeader:     sync.Mutex{},
		inElection:   false,
		MuInElection: sync.Mutex{},
		answerChan:   nil,
		MuAnswerChan: sync.Mutex{},
	}
}

func (m *Monitor) Start(ctx context.Context) {
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		m.startTCPServer(ctx)
	}()

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
	wg.Add(1)
	go func() {
		defer wg.Done()
		m.listenHeartbeats(conn, ctx)
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		m.checkLeaderAlive(ctx)
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		m.sendHeartbeat(ctx)
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		m.checkWorkers(cli, ctx)
	}()

	wg.Wait()
	m.MuAnswerChan.Lock()
	if m.answerChan != nil {
		close(m.answerChan)
	}
	m.MuAnswerChan.Unlock()
	log.Infof("Monitor %s exiting", MONITOR_ID)
}

func (m *Monitor) sendHeartbeat(ctx context.Context) {
	name := m.name()
	peerAddrs := m.peerAddrs()
	log.Infof("Sending heartbeat to peers: %s", peerAddrs)
	utils.SendHeartbeat(name, peerAddrs, log, ctx)
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

func (m *Monitor) listenHeartbeats(conn *net.UDPConn, ctx context.Context) {
	buffer := make([]byte, MaxUDPMessageSize)
	for {
		select {
		case <-ctx.Done():
			log.Infof("Stopping listenHeartbeats")
			return
		default:
			n, addr, err := conn.ReadFromUDP(buffer)
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

			msg := string(buffer[HEADER_SIZE : HEADER_SIZE+msgLen])
			parts := strings.Split(msg, "|")
			id := parts[0]

			workerType := WORKER
			if strings.Contains(id, "client") {
				workerType = CLIENT
				m.MuWorkers.Lock()
				client, exists := m.Workers[id]
				m.MuWorkers.Unlock()
				if exists && client.Status == EXITED {
					log.Infof("Client %s already marked as exited", id)
					continue
				}
				if len(parts) > 1 && parts[1] == "e" {
					m.MuLeader.Lock()
					if m.isLeader() {
						log.Infof("%s exited, sending ACK", id)
						packet := []byte("ACK")
						utils.WriteUDP(addr, log, packet, conn)
					}
					m.MuLeader.Unlock()
					m.MuWorkers.Lock()
					if exists && !(client.Status == EXITED) {
						m.Workers[id] = WorkerStatus{LastSeen: client.LastSeen, Type: CLIENT, Status: EXITED}
						log.Infof("%s exited", id)
					}
					m.MuWorkers.Unlock()
					continue
				}

			}
			if strings.Contains(id, "monitor") {
				workerType = MONITOR
			}
			if strings.Contains(id, "sentiment_server") {
				workerType = SENTIMENT_SERVER
			}

			m.MuWorkers.Lock()
			m.Workers[id] = WorkerStatus{LastSeen: time.Now(), Type: workerType, Status: RUNNING}
			m.MuWorkers.Unlock()
		}
	}
}

func (m *Monitor) checkWorkers(cli *client.Client, ctx context.Context) {
	ticker := time.NewTicker(CHECK_INTERVAL)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			log.Infof("Stopping checkWorkers")
			return
		case <-ticker.C:

			workerStatus := make(map[string]WorkerStatus)

			m.MuWorkers.Lock()
			for k, v := range m.Workers {
				workerStatus[k] = v
			}
			m.MuWorkers.Unlock()
			now := time.Now()

			for id, status := range workerStatus {
				if now.Sub(status.LastSeen) > m.Timeout && !(status.Status == EXITED) {
					isLeader := false
					m.MuLeader.Lock()
					isLeader = m.isLeader()
					m.MuLeader.Unlock()
					if shouldRestart(isLeader, status) {
						log.Infof("%s not responding. Restarting...", id)
						m.MuWorkers.Lock()
						m.Workers[id] = WorkerStatus{LastSeen: status.LastSeen, Type: status.Type, Status: STARTING}
						m.MuWorkers.Unlock()
						m.restartContainer(cli, id)
					} else {
						log.Infof("%s not responding.", id)
					}
				}
			}
		}
	}
}

func shouldRestart(isLeader bool, status WorkerStatus) bool {
	now := time.Now()
	return isLeader && (status.Status == RUNNING || (status.Status == STARTING && now.Sub(status.LastSeen) > STARTING_TIMEOUT))
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

func sendMessage(addr, msg string) {
	var conn net.Conn
	var err error
	for range 5 {
		conn, err = net.Dial("tcp", addr)
		if err == nil {
			break
		}
		time.Sleep(1 * time.Second)
	}
	if err != nil || conn == nil {
		log.Warnf("Could not connect to %s: %v", addr, err)
		return
	}
	defer conn.Close()
	packet := append([]byte{byte(len(msg))}, []byte(msg)...)
	utils.WriteToConn(addr, log, packet, conn)
}

func (m *Monitor) startElection() {
	m.MuInElection.Lock()
	if m.inElection {
		//log.Infof("Monitor %s already in election", MONITOR_ID)
		m.MuInElection.Unlock()
		return
	}
	m.inElection = true
	m.MuInElection.Unlock()
	log.Infof("Monitor %s starting election", MONITOR_ID)

	hasMaxId := true
	myID, err := strconv.Atoi(MONITOR_ID)
	unwrap(err, "Failed to convert MONITOR_ID to int")

	answerCh := make(chan bool, 1)
	m.MuAnswerChan.Lock()
	m.answerChan = answerCh
	m.MuAnswerChan.Unlock()

	for id, addr := range m.Peers {
		peerID, err := strconv.Atoi(id)
		unwrap(err, "Failed to convert peerID to int")
		if peerID > myID {
			go sendMessage(addr, fmt.Sprintf("ELECTION|%s", MONITOR_ID))
			hasMaxId = false
		}
	}
	if hasMaxId {
		log.Infof("I have the highest ID. Selecting myself as leader.")
		m.SelectMyselfAsLeader()
		return
	}

	log.Infof("Waiting for ANSWER from peers")

	select {
	case <-answerCh:
		log.Infof("Received ANSWER")
		m.MuInElection.Lock()
		m.inElection = false
		m.MuInElection.Unlock()
		m.MuAnswerChan.Lock()
		m.answerChan = nil
		m.MuAnswerChan.Unlock()
	case <-time.After(ELECTION_TIMEOUT):
		log.Infof("Timeout without ANSWER, selecting myself as leader.")
		m.SelectMyselfAsLeader()
	}

}

func (m *Monitor) SelectMyselfAsLeader() {
	m.NewLeader(MONITOR_ID)
}

func (m *Monitor) NewLeader(leaderId string) {
	m.MuLeader.Lock()
	m.LeaderID = leaderId
	m.MuLeader.Unlock()
	if leaderId == MONITOR_ID {
		m.announceCoordinatorToPeers()
	}
	m.MuInElection.Lock()
	m.inElection = false
	m.MuInElection.Unlock()
	m.MuAnswerChan.Lock()
	if m.answerChan != nil && leaderId != MONITOR_ID {
		select {
		case m.answerChan <- true:
		default:
			// no bloquear
		}
	}
	m.answerChan = nil
	m.MuAnswerChan.Unlock()
}

func (m *Monitor) announceCoordinatorToPeers() {
	for _, addr := range m.Peers {
		m.announceCoordinator(addr)
	}
}

func (m *Monitor) announceCoordinator(addr string) {
	log.Infof("Announcing myself as coordinator to %s", addr)
	go sendMessage(addr, fmt.Sprintf("COORDINATOR|%s", MONITOR_ID))
}
func (m *Monitor) startTCPServer(ctx context.Context) {
	listener, err := net.Listen("tcp", ":"+m.port)
	if err != nil {
		log.Fatalf("TCP Listen error: %v", err)
	}
	log.Infof("TCP server on port %s", m.port)

	for {
		select {
		case <-ctx.Done():
			log.Infof("Stopping TCP server")
			listener.Close()
			return
		default:
			conn, err := listener.Accept()
			if err != nil {
				log.Warnf("Accept error: %v", err)
				continue
			}
			go m.handleConnection(conn, ctx)
		}
	}
}

func (m *Monitor) handleConnection(conn net.Conn, ctx context.Context) {
	defer conn.Close()

	msg, err := utils.ReceiveTCPMessage(conn, ctx)
	if err != nil {
		log.Errorf("Error receiving message: %v", err)
		return
	}
	parts := strings.Split(msg, "|")

	switch parts[0] {
	case "ELECTION":
		senderID := parts[1]
		go sendMessage(m.Peers[senderID], fmt.Sprintf("ANSWER|%s", MONITOR_ID))

		log.Infof("Received ELECTION from %s", senderID)
		go m.startElection()
	case "ANSWER":
		log.Infof("Received ANSWER from %s", parts[1])
		m.MuAnswerChan.Lock()
		if m.answerChan == nil {
			select {
			case m.answerChan <- true:
			default:
				// no bloquear
			}
		}
		m.MuAnswerChan.Unlock()
	case "COORDINATOR":
		newLeader := parts[1]
		myID, err := strconv.Atoi(MONITOR_ID)
		unwrap(err, "Failed to convert MONITOR_ID to int")
		newLeaderID, err := strconv.Atoi(newLeader)
		unwrap(err, "Failed to convert newLeaderID to int")

		if newLeaderID < myID {
			log.Infof("Ignoring COORDINATOR %s because I have higher ID", newLeader)
			return
		}
		log.Infof("New coordinator is %s", newLeader)
		m.NewLeader(newLeader)
	}
}

func (m *Monitor) checkLeaderAlive(ctx context.Context) {
	ticker := time.NewTicker(CHECK_INTERVAL)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			log.Infof("Stopping checkWorkers")
			return
		case <-ticker.C:
			var leader string
			var isLeader bool
			m.MuLeader.Lock()
			leader = m.LeaderID
			isLeader = m.isLeader()

			m.MuLeader.Unlock()
			if m.LeaderID == "" {
				log.Infof("No leader elected yet. Starting election.")
				go m.startElection()
				continue
			}

			if isLeader {
				continue
			}

			m.MuWorkers.Lock()
			status, ok := m.Workers["monitor"+leader]
			m.MuWorkers.Unlock()

			if !ok || time.Since(status.LastSeen) > m.Timeout {
				log.Warnf("Leader %s not responding. Starting election.", leader)
				go m.startElection()
			}
		}
	}
}
