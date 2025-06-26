package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var basePorts = map[string]int{
	"TestMonitor/StopMonitor1":              8070,
	"TestMonitor/LeaderElectionOneMonitor":  8075,
	"TestMonitor/LeaderElectionInOrder":     8080,
	"TestMonitor/LeaderElectionStopMonitor": 8085,
	"TestMonitor/LeaderElectionStopLeader":  8090,
}

func TestMonitor(t *testing.T) {

	t.Run("StopMonitor1", func(t *testing.T) {

		monitorCount := 1
		ids, ports, peers := getConfig(t.Name(), monitorCount)

		time.Sleep(500 * time.Millisecond)
		wg := &sync.WaitGroup{}
		monitor, stopM1 := startMonitor(ids[0], ports[0], peers, wg)

		time.Sleep(500 * time.Millisecond)

		t.Logf("Stopping monitor %s", ids[0])
		stopM1()

		wg.Wait()
		assert.NotNil(t, monitor)

	})

	t.Run("LeaderElectionOneMonitor", func(t *testing.T) {

		monitorCount := 1
		ids, ports, peers := getConfig(t.Name(), monitorCount)

		wg := &sync.WaitGroup{}
		monitor1, stopM1 := startMonitor(ids[0], ports[0], peers, wg)

		leader := waitForLeader(t, "", monitor1)
		assert.Equal(t, leader, "1", "El primer monitor debería ser el líder")

		time.Sleep(1 * time.Second)
		stopM1()
		wg.Wait()

	})

	t.Run("LeaderElectionInOrder", func(t *testing.T) {

		monitorCount := 3
		ids, ports, peers := getConfig(t.Name(), monitorCount)

		wg := &sync.WaitGroup{}
		monitor1, stopM1 := startMonitor(ids[0], ports[0], peers, wg)

		verifyLeader(t, monitor1, "1", "", false)

		monitor2, stopM2 := startMonitor(ids[1], ports[1], peers, wg)

		verifyLeader(t, monitor2, "2", "", false)
		verifyLeader(t, monitor1, "2", "1", false)

		monitor3, stopM3 := startMonitor(ids[2], ports[2], peers, wg)

		verifyLeader(t, monitor3, "3", "", false)
		verifyLeader(t, monitor2, "3", "2", false)
		verifyLeader(t, monitor1, "3", "2", false)

		stopM1()
		stopM2()
		stopM3()
		wg.Wait()

	})

	t.Run("LeaderElectionStopMonitor", func(t *testing.T) {

		monitorCount := 3
		ids, ports, peers := getConfig(t.Name(), monitorCount)

		wg := &sync.WaitGroup{}
		monitor1, stopM1 := startMonitor(ids[0], ports[0], peers, wg)
		monitor2, stopM2 := startMonitor(ids[1], ports[1], peers, wg)
		monitor3, stopM3 := startMonitor(ids[2], ports[2], peers, wg)

		verifyLeader(t, monitor3, "3", "", false)
		verifyLeader(t, monitor2, "3", "", false)
		verifyLeader(t, monitor1, "3", "", false)

		stopM1()

		waitUntilRestarting(monitor1, monitor3)
		monitor1, stopM1 = startMonitor(ids[0], ports[0], peers, wg) // simulo levantarlo nuevamente

		verifyLeader(t, monitor1, "3", "", true)
		verifyLeader(t, monitor3, "3", "", true)
		verifyLeader(t, monitor2, "3", "", true)

		stopM1()
		stopM2()
		stopM3()
		wg.Wait()

	})

	t.Run("LeaderElectionStopLeader", func(t *testing.T) {

		monitorCount := 3
		ids, ports, peers := getConfig(t.Name(), monitorCount)

		wg := &sync.WaitGroup{}
		monitor1, stopM1 := startMonitor(ids[0], ports[0], peers, wg)
		monitor2, stopM2 := startMonitor(ids[1], ports[1], peers, wg)
		monitor3, stopM3 := startMonitor(ids[2], ports[2], peers, wg)

		verifyLeader(t, monitor3, "3", "", false)
		verifyLeader(t, monitor2, "3", "", false)
		verifyLeader(t, monitor1, "3", "", false)

		stopM3()

		waitUntilRestarting(monitor3, monitor2)
		verifyLeader(t, monitor2, "2", "3", true)
		verifyLeader(t, monitor1, "2", "3", true)

		monitor3, stopM3 = startMonitor(ids[2], ports[2], peers, wg) // simulo levantarlo nuevamente

		verifyLeader(t, monitor3, "3", "", true)
		verifyLeader(t, monitor1, "3", "2", true)
		verifyLeader(t, monitor2, "3", "2", true)

		stopM1()
		stopM2()
		stopM3()
		wg.Wait()

	})

}

func startMonitor(id, port, peers string, wg *sync.WaitGroup) (*Monitor, context.CancelFunc) {
	monitor := NewMonitor(id, port, peers)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	wg.Add(1)
	go func() {
		defer wg.Done()
		monitor.Start(ctx)
	}()
	return monitor, stop
}

func getConfig(testName string, monitorCount int) ([]string, []string, string) {
	basePort := basePorts[testName]
	ports := make([]string, monitorCount)
	peers := ""
	ids := make([]string, monitorCount)

	for i := range monitorCount {
		id := i + 1
		port := basePort + id
		ports[i] = fmt.Sprintf("%d", port)
		peers += fmt.Sprintf("%d:localhost:%d", id, port)
		if i < monitorCount-1 {
			peers += ","
		}
		ids[i] = fmt.Sprintf("%d", id)
	}
	return ids, ports, peers
}

func waitForLeader(t *testing.T, previousVal string, m *Monitor) string {
	t.Helper()

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	timeoutChan := time.After(END_ELECTION_TIMEOUT * 2)
	for {
		select {
		case <-ticker.C:
			m.MuLeader.Lock()
			leader := m.LeaderID
			m.MuLeader.Unlock()
			m.MuInElection.Lock()
			inElection := m.inElection
			m.MuInElection.Unlock()

			if leader != previousVal && !inElection {
				return leader
			}
		case <-timeoutChan:
			t.Fatalf("Timeout esperando a que el monitor %s tenga líder", m.id)
			return ""
		}
	}
}

func verifyLeader(t *testing.T, m *Monitor, expectedLeader string, previousVal string, retry bool) {
	leader := waitForLeader(t, previousVal, m)
	if leader != expectedLeader && retry {
		leader = waitForLeader(t, previousVal, m)
	}

	assert.Equal(t, leader, expectedLeader)

	if leader == expectedLeader {
		t.Logf("%s | Monitor %s es el líder", m.id, leader)
	}

}

func waitUntilRestarting(m *Monitor, peer *Monitor) {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	timeoutChan := time.After(TIMEOUT * 4)

	for {
		select {
		case <-ticker.C:
			peer.MuServices.Lock()
			status, exists := peer.Services["monitor"+m.id]
			peer.MuServices.Unlock()
			if exists && status.Status == STARTING {
				return
			}
		case <-timeoutChan:
			panic(fmt.Sprintf("Timeout esperando a que el monitor %s sea restarted", m.id))
		}
	}
}
