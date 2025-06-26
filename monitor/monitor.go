package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"maps"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/client"
	"github.com/ptourne/sistemas-distribuidos-1/common/logger"
	"github.com/ptourne/sistemas-distribuidos-1/common/utils"
)

const HEADER_SIZE = 1
const MaxUDPMessageSize = 1024
const CHECK_INTERVAL = 3 * time.Second                    // ToDo: ajustar
const TIMEOUT = 10 * time.Second                          // ToDo: ajustar
const SENTIMENT_SERVER_STARTING_TIMEOUT = 1 * time.Minute // ToDo: ajustar
const STARTING_TIMEOUT = 20 * time.Second                 // ToDo: ajustar
const ELECTION_TIMEOUT = 5 * time.Second                  // ToDo: ajustar
const HEARTBEAT_INTERVAL = 500 * time.Millisecond         // ToDo: ajustar
const READ_TIMEOUT = 1 * time.Second                      // ToDo: ajustar

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

type ServiceStatus struct {
	LastSeen time.Time
	Type     WorkerType
	Status   Status
}

type Monitor struct {
	id                   string
	Services             map[string]ServiceStatus
	MuServices           sync.Mutex
	Timeout              time.Duration
	port                 string
	LeaderID             string
	MuLeader             sync.Mutex
	Peers                map[string]string   // id -> address (IP:PORT)
	PeersConnElection    map[string]net.Conn // id -> connection
	MuPeersConnElection  sync.Mutex
	PeersConnHeartbeat   map[string]net.Conn // id -> connection
	MuPeersConnHeartbeat sync.Mutex
	inElection           bool
	MuInElection         sync.Mutex
	answerChan           chan bool
	MuAnswerChan         sync.Mutex
	electionConnLocks    map[string]*sync.Mutex
	MuElectionConnLocks  sync.Mutex
	heartbeatConnLocks   map[string]*sync.Mutex
	MuHeartbeatConnLocks sync.Mutex
	log                  *logger.ConsoleLogger
}

func NewMonitor(id, port, rawPeers string) *Monitor {
	peers := make(map[string]string)
	for _, peer := range strings.Split(rawPeers, ",") {
		parts := strings.Split(peer, ":")
		if len(parts) == 3 && parts[0] != id {
			peers[parts[0]] = fmt.Sprintf("%s:%s", parts[1], parts[2])
		}
	}

	log := logger.NewConsoleLogger(fmt.Sprintf("monitor_%s", id), logger.Info)

	services := getServicesData(log, id)

	return &Monitor{
		id:                   id,
		Services:             services,
		MuServices:           sync.Mutex{},
		Timeout:              TIMEOUT,
		port:                 port,
		Peers:                peers,
		PeersConnElection:    make(map[string]net.Conn),
		MuPeersConnElection:  sync.Mutex{},
		PeersConnHeartbeat:   make(map[string]net.Conn),
		MuPeersConnHeartbeat: sync.Mutex{},
		LeaderID:             "",
		MuLeader:             sync.Mutex{},
		inElection:           false,
		MuInElection:         sync.Mutex{},
		answerChan:           nil,
		MuAnswerChan:         sync.Mutex{},
		electionConnLocks:    make(map[string]*sync.Mutex),
		MuElectionConnLocks:  sync.Mutex{},
		heartbeatConnLocks:   make(map[string]*sync.Mutex),
		MuHeartbeatConnLocks: sync.Mutex{},
		log:                  log,
	}
}

func getServicesData(log *logger.ConsoleLogger, id string) map[string]ServiceStatus {
	monitorServices := make(map[string]ServiceStatus)
	servicesEnv := os.Getenv("SERVICES")
	if servicesEnv == "" {
		return monitorServices
	}
	log.Infof("SERVICES: %s", servicesEnv)
	services := strings.Split(servicesEnv, ",")
	for _, service := range services {
		serviceType := WORKER
		if strings.Contains(service, "client") {
			serviceType = CLIENT
		}
		if strings.Contains(service, "sentiment_server") {
			serviceType = SENTIMENT_SERVER
		}
		if strings.Contains(service, "monitor") {
			serviceType = MONITOR
			if strings.Contains(service, id) {
				continue // No incluir el monitor actual en la lista de servicios
			}
		}
		monitorServices[service] = ServiceStatus{
			LastSeen: time.Now(),
			Type:     serviceType,
			Status:   STARTING,
		}
	}
	return monitorServices
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
		m.log.Fatalf("Error creando cliente Docker: %v", err)
	}

	m.log.Infof("Monitor listening on %s", addr.String())
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
		m.sendHeartbeatToPeers(ctx)
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		m.checkServices(cli, ctx)
	}()

	wg.Wait()
	m.MuAnswerChan.Lock()
	if m.answerChan != nil {
		close(m.answerChan)
	}
	m.MuAnswerChan.Unlock()
	m.MuPeersConnElection.Lock()
	for _, conn := range m.PeersConnElection {
		conn.Close()
	}
	m.MuPeersConnElection.Unlock()
	m.log.Infof("Monitor %s exiting", m.id)
}

func (m *Monitor) sendHeartbeatToPeers(ctx context.Context) {
	msg := fmt.Sprintf("HEARTBEAT|%s", m.id)
	ticker := time.NewTicker(HEARTBEAT_INTERVAL)
	for {
		select {
		case <-ctx.Done():
			m.log.Infof("Stopping sendHeartbeatToPeers")
			ticker.Stop()
			return
		case <-ticker.C:
			for peer, _ := range m.Peers {
				m.sendMsgToPeer(peer, msg, false)
			}
		}
	}
}

func (m *Monitor) restartContainer(cli *client.Client, containerName string) {
	ctx := context.Background()
	stopOptions := container.StopOptions{
		Timeout: nil,
		Signal:  "SIGTERM",
	}
	err := cli.ContainerRestart(ctx, containerName, stopOptions)
	if err != nil {
		m.log.Errorf("Failed to restart %s: %v", containerName, err)
	} else {
		m.log.Infof("Restarting Container %s", containerName)
	}
}

func (m *Monitor) listenHeartbeats(conn *net.UDPConn, ctx context.Context) {
	buffer := make([]byte, MaxUDPMessageSize)

	for {
		select {
		case <-ctx.Done():
			m.log.Infof("Stopping listenHeartbeats")
			return
		default:
			if err := conn.SetReadDeadline(time.Now().Add(READ_TIMEOUT)); err != nil {
				m.log.Warnf("Error setting read deadline: %v", err)
				continue
			}
			n, addr, err := conn.ReadFromUDP(buffer)
			if err != nil {
				if ne, ok := err.(net.Error); ok && ne.Timeout() {
					select {
					case <-ctx.Done():
						m.log.Infof("Stopping listenHeartbeats")
						return
					default:
						continue
					}
				}
				m.log.Warnf("Error reading UDP: %v", err)
				continue
			}

			if n < HEADER_SIZE {
				m.log.Errorf("Received incomplete message: got %d bytes, expected at least %d", n, HEADER_SIZE)
				continue
			}

			msgLen := int(buffer[0])
			if n-HEADER_SIZE != msgLen {
				m.log.Errorf("Received incomplete message: got %d bytes, expected %d", n-HEADER_SIZE, msgLen)
				continue
			}

			msg := string(buffer[HEADER_SIZE : HEADER_SIZE+msgLen])
			parts := strings.Split(msg, "|")
			id := parts[0]

			serviceType := WORKER
			if strings.Contains(id, "client") {
				serviceType = CLIENT
				m.MuServices.Lock()
				client, exists := m.Services[id]
				m.MuServices.Unlock()
				if exists && client.Status == EXITED {
					m.log.Infof("Client %s already marked as exited", id)
					continue
				}
				if len(parts) > 1 && parts[1] == "e" {
					m.MuLeader.Lock()
					if m.isLeader() {
						m.log.Infof("%s exited, sending ACK", id)
						packet := []byte("ACK")
						utils.WriteUDP(addr, m.log, packet, conn)
					}
					m.MuLeader.Unlock()
					m.MuServices.Lock()
					if exists && !(client.Status == EXITED) {
						m.Services[id] = ServiceStatus{LastSeen: client.LastSeen, Type: CLIENT, Status: EXITED}
						m.log.Infof("%s exited", id)
					}
					m.MuServices.Unlock()
					continue
				}

			}
			if strings.Contains(id, "sentiment_server") {
				serviceType = SENTIMENT_SERVER
			}

			m.MuServices.Lock()
			m.Services[id] = ServiceStatus{LastSeen: time.Now(), Type: serviceType, Status: RUNNING}
			m.MuServices.Unlock()
		}
	}
}

func (m *Monitor) checkServices(cli *client.Client, ctx context.Context) {
	ticker := time.NewTicker(CHECK_INTERVAL)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			m.log.Infof("Stopping checkServices")
			return
		case <-ticker.C:

			serviceStatus := make(map[string]ServiceStatus)

			m.MuServices.Lock()
			maps.Copy(serviceStatus, m.Services)
			m.MuServices.Unlock()
			now := time.Now()

			for id, status := range serviceStatus {
				if now.Sub(status.LastSeen) > m.Timeout && !(status.Status == EXITED) {
					isLeader := false
					m.MuLeader.Lock()
					isLeader = m.isLeader()
					m.MuLeader.Unlock()
					if m.shouldRestart(isLeader, status, id) {
						m.log.Infof("%s not responding", id)
						m.MuServices.Lock()
						m.Services[id] = ServiceStatus{LastSeen: status.LastSeen, Type: status.Type, Status: STARTING}
						m.MuServices.Unlock()
						go m.restartContainer(cli, id)
					}
				}
			}
		}
	}
}

func (m *Monitor) shouldRestart(isLeader bool, status ServiceStatus, id string) bool {
	now := time.Now()
	return isLeader &&
		(status.Status == RUNNING ||
			(status.Type == SENTIMENT_SERVER && status.Status == STARTING && now.Sub(status.LastSeen) > SENTIMENT_SERVER_STARTING_TIMEOUT) ||
			(status.Type != SENTIMENT_SERVER && status.Status == STARTING && now.Sub(status.LastSeen) > STARTING_TIMEOUT) ||
			(status.Type == MONITOR && m.isLeaderDown(id)))
}

func (m *Monitor) isLeaderDown(name string) bool {
	peer := strings.TrimPrefix(name, "monitor")
	_, err := m.connectTo(m.Peers[peer])
	return err != nil
}

func (m *Monitor) isLeader() bool {
	return m.LeaderID == m.id
}

func unwrap(err error, msg string) {
	if err != nil {
		//m.log.Fatalf("%s: %s", msg, err)
		panic(err)

	}
}

func (m *Monitor) sendMsgToPeer(peer, msg string, electionMsg bool) {
	var lock *sync.Mutex
	if electionMsg {
		lock = m.getElectionConnLock(peer)
	} else {
		lock = m.getHeartbeatConnLock(peer)
	}
	lock.Lock()
	defer lock.Unlock()

	var conn net.Conn
	var err error
	var c net.Conn
	var exists bool
	addr := m.Peers[peer]
	if electionMsg {
		m.MuPeersConnElection.Lock()
		c, exists = m.PeersConnElection[peer]
		m.MuPeersConnElection.Unlock()
	} else {
		m.MuPeersConnHeartbeat.Lock()
		c, exists = m.PeersConnHeartbeat[peer]
		m.MuPeersConnHeartbeat.Unlock()
	}
	reconnected := false
	if exists {
		conn = c
	} else {
		conn, err = m.connectToPeer(peer, addr, electionMsg)
		if err != nil || conn == nil {
			m.log.Warnf("Could not connect to peer %s with addr=%s: %v", peer, addr, err)
			return
		}
		reconnected = true
	}
	packet := append([]byte{byte(len(msg))}, []byte(msg)...)
	err = m.sendToPeer(peer, addr, packet, conn, electionMsg)

	if (err != nil && reconnected) || err == nil {
		return
	}

	// Retry If the connection was not reconnected
	conn, err = m.connectToPeer(peer, addr, electionMsg)
	if err != nil || conn == nil {
		m.log.Warnf("Could not connect to %s: %v", addr, err)
		return
	}
	m.sendToPeer(peer, addr, packet, conn, electionMsg)
}

func (m *Monitor) getElectionConnLock(peer string) *sync.Mutex {
	m.MuElectionConnLocks.Lock()
	defer m.MuElectionConnLocks.Unlock()
	if m.electionConnLocks == nil {
		m.electionConnLocks = make(map[string]*sync.Mutex)
	}
	lock, ok := m.electionConnLocks[peer]
	if !ok {
		lock = &sync.Mutex{}
		m.electionConnLocks[peer] = lock
	}
	return lock
}

func (m *Monitor) getHeartbeatConnLock(peer string) *sync.Mutex {
	m.MuHeartbeatConnLocks.Lock()
	defer m.MuHeartbeatConnLocks.Unlock()
	if m.heartbeatConnLocks == nil {
		m.heartbeatConnLocks = make(map[string]*sync.Mutex)
	}
	lock, ok := m.heartbeatConnLocks[peer]
	if !ok {
		lock = &sync.Mutex{}
		m.heartbeatConnLocks[peer] = lock
	}
	return lock
}

func (m *Monitor) sendToPeer(peer, addr string, packet []byte, conn net.Conn, electionMsg bool) error {
	err := utils.WriteToConn(addr, packet, conn)
	if err == nil {
		return nil
	}
	m.log.Warnf("Error sending to peer %s with addr=%s: %v", peer, addr, err)
	conn.Close()
	if electionMsg {
		m.MuPeersConnElection.Lock()
		delete(m.PeersConnElection, peer)
		m.MuPeersConnElection.Unlock()
	} else {
		m.MuPeersConnHeartbeat.Lock()
		delete(m.PeersConnHeartbeat, peer)
		m.MuPeersConnHeartbeat.Unlock()
	}
	return err
}

func (m *Monitor) connectToPeer(peer, addr string, electionMsg bool) (net.Conn, error) {
	conn, err := m.connectTo(addr)
	if err == nil {
		if electionMsg {
			m.MuPeersConnElection.Lock()
			m.PeersConnElection[peer] = conn
			m.MuPeersConnElection.Unlock()
		} else {
			m.MuPeersConnHeartbeat.Lock()
			m.PeersConnHeartbeat[peer] = conn
			m.MuPeersConnHeartbeat.Unlock()
		}
	}
	return conn, err
}

func (m *Monitor) connectTo(addr string) (net.Conn, error) {
	var conn net.Conn
	var err error
	for range 5 {
		conn, err = net.Dial("tcp", addr)
		if err == nil {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	return conn, err
}

func (m *Monitor) startElection() {
	m.MuInElection.Lock()
	if m.inElection {
		//m.log.Infof("Monitor %s already in election", m.id)
		m.MuInElection.Unlock()
		return
	}
	m.inElection = true
	m.MuInElection.Unlock()
	m.log.Infof("Starting election")

	hasMaxId := true
	myID, err := strconv.Atoi(m.id)
	unwrap(err, "Failed to convert MONITOR_ID to int")

	m.MuAnswerChan.Lock()
	m.answerChan = make(chan bool, 1)
	m.MuAnswerChan.Unlock()

	for id, _ := range m.Peers {
		peerID, err := strconv.Atoi(id)
		unwrap(err, "Failed to convert peerID to int")
		if peerID > myID {
			m.log.Infof("Sending ELECTION to %s", id)
			go m.sendMsgToPeer(id, fmt.Sprintf("ELECTION|%s", m.id), true)
			hasMaxId = false
		}
	}
	if hasMaxId {
		m.log.Infof("I have the highest ID. Selecting myself as leader.")
		m.SelectMyselfAsLeader()
		return
	}

	m.log.Infof("Waiting for ANSWER from peers")

	m.MuAnswerChan.Lock()
	ch := m.answerChan
	m.MuAnswerChan.Unlock()

	select {
	case <-ch:
		m.log.Infof("Received ANSWER")
		m.MuAnswerChan.Lock()
		m.answerChan = nil
		m.MuAnswerChan.Unlock()
	case <-time.After(ELECTION_TIMEOUT):
		m.log.Infof("Timeout without ANSWER, selecting myself as leader.")
		m.SelectMyselfAsLeader()
	}

}

func (m *Monitor) SelectMyselfAsLeader() {
	m.NewLeader(m.id)
}

func (m *Monitor) NewLeader(leaderId string) {
	m.MuLeader.Lock()
	m.LeaderID = leaderId
	m.MuLeader.Unlock()
	if leaderId == m.id {
		m.announceCoordinatorToPeers()
	}
	m.MuInElection.Lock()
	m.inElection = false
	m.MuInElection.Unlock()
	m.MuAnswerChan.Lock()
	if m.answerChan != nil && leaderId != m.id {
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
	for peer, _ := range m.Peers {
		m.announceCoordinator(peer)
	}
}

func (m *Monitor) announceCoordinator(peer string) {
	m.log.Infof("Announcing myself as coordinator to %s", peer)
	go m.sendMsgToPeer(peer, fmt.Sprintf("COORDINATOR|%s", m.id), true)
}

func (m *Monitor) startTCPServer(ctx context.Context) {
	listener, err := net.Listen("tcp", ":"+m.port)
	if err != nil {
		m.log.Fatalf("TCP Listen error: %v", err)
	}
	m.log.Infof("TCP server on port %s", m.port)

	wg := sync.WaitGroup{}

	go func() {
		<-ctx.Done()
		m.log.Infof("Stopping TCP listener")
		listener.Close()
	}()

outerLoop:
	for {
		conn, err := listener.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				break outerLoop
			default:
				m.log.Warnf("Accept error: %v", err)
				continue
			}
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			m.handleConnection(conn, ctx)
			conn.Close()
		}()
	}
	wg.Wait()
	m.log.Infof("Stopping TCP server")
}

func (m *Monitor) handleConnection(conn net.Conn, ctx context.Context) {
	defer conn.Close()

	peer := ""

	for {
		select {
		case <-ctx.Done():
			m.log.Infof("Stopping handleConnection | peer: %s", peer)
			return
		default:
			msg, err := utils.ReceiveTCPMessage(conn)
			if err != nil {
				if ctx.Err() != nil {
					m.log.Infof("Error receiving from TCP conn: %s. Stopping handleConnection | peer: %s", err, peer)
					return
				}
				if errors.Is(err, io.EOF) {
					m.log.Infof("Connection closed by peer. Stopping handleConnection | peer: %s", peer)
					return
				}
				continue
			}
			parts := strings.Split(msg, "|")
			senderID := parts[1]
			peer = senderID
			sender := "monitor" + senderID
			switch parts[0] {
			case "ELECTION":
				go m.sendMsgToPeer(senderID, fmt.Sprintf("ANSWER|%s", m.id), true)

				m.log.Infof("Received ELECTION from %s", senderID)
				go m.startElection()
			case "ANSWER":
				m.log.Infof("Received ANSWER from %s", parts[1])
				m.MuAnswerChan.Lock()
				if m.answerChan != nil {
					select {
					case m.answerChan <- true:
					default:
						// no bloquear
					}
				}
				m.MuAnswerChan.Unlock()
			case "COORDINATOR":
				myID, err := strconv.Atoi(m.id)
				unwrap(err, "Failed to convert MONITOR_ID to int")
				newLeaderID, err := strconv.Atoi(senderID)
				unwrap(err, "Failed to convert newLeaderID to int")

				if newLeaderID < myID {
					m.log.Infof("Ignoring COORDINATOR %s because I have higher ID", senderID)
					go m.startElection()
				} else {
					m.log.Infof("New coordinator is %s", senderID)
					m.NewLeader(senderID)
				}

			case "HEARTBEAT":
				//m.log.Infof("Received HEARTBEAT from %s", senderID)
				m.MuServices.Lock()
				m.Services[sender] = ServiceStatus{LastSeen: time.Now(), Type: MONITOR, Status: RUNNING}
				m.MuServices.Unlock()
				continue
			}
			// Update service status if an election message is received
			m.MuServices.Lock()
			m.Services[sender] = ServiceStatus{LastSeen: time.Now(), Type: MONITOR, Status: RUNNING}
			m.MuServices.Unlock()
		}
	}
}

func (m *Monitor) checkLeaderAlive(ctx context.Context) {
	ticker := time.NewTicker(CHECK_INTERVAL)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			m.log.Infof("Stopping checkLeaderAlive")
			return
		case <-ticker.C:
			var leader string
			var isLeader bool
			m.MuLeader.Lock()
			leader = m.LeaderID
			isLeader = m.isLeader()

			m.MuLeader.Unlock()
			if leader == "" {
				//m.log.Infof("No leader elected yet. Starting election.")
				go m.startElection()
				continue
			}

			if isLeader {
				continue
			}

			m.MuServices.Lock()
			status, ok := m.Services["monitor"+leader]
			last_seen := time.Since(status.LastSeen)
			m.MuServices.Unlock()
			if !ok || last_seen > m.Timeout {
				m.log.Warnf("Leader %s not responding.", leader)
				go m.startElection()
			}
		}
	}
}
