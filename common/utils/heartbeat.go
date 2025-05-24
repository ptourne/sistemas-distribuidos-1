package utils

import (
	"net"
	"strings"
	"time"

	"github.com/ptourne/sistemas-distribuidos-1/common/logger"
)

const HEATBEAT_INTERVAL = 1 * time.Second // ToDo: ajustar

func SendHeartbeat(id string, addrs string, log *logger.ConsoleLogger) {
	addresses := strings.Split(addrs, ",")

	udpAddrs := []*net.UDPAddr{}
	for _, addr := range addresses {
		udpAddr, err := net.ResolveUDPAddr("udp", strings.TrimSpace(addr))
		if err != nil {
			log.Errorf("Failed to resolve UDP address %s: %v", addr, err)
			continue
		}
		udpAddrs = append(udpAddrs, udpAddr)
	}
	for {
		msg := []byte(id)
		msgLen := len(msg)

		if msgLen > 255 {
			log.Fatalf("ID too long: %d bytes", msgLen)
		}

		packet := append([]byte{byte(msgLen)}, msg...)

		for _, addr := range udpAddrs {
			conn, err := net.DialUDP("udp", nil, addr)
			if err != nil {
				log.Errorf("Failed to connect to monitor %s: %v", addr.String(), err)
				continue
			}

			sendHeartbeat(addr, log, packet, conn)

			conn.Close()
		}

		time.Sleep(HEATBEAT_INTERVAL)
	}
}

func sendHeartbeat(addr *net.UDPAddr, log *logger.ConsoleLogger, packet []byte, conn net.Conn) {
	totalSent := 0

	for totalSent < len(packet) {
		sent, err := conn.Write(packet[totalSent:])
		if err != nil {
			log.Errorf("Failed to send heartbeat to %s: %v", addr.String(), err)
			continue
		}
		totalSent += sent
	}
}
