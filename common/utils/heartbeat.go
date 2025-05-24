package utils

import (
	"net"
	"time"

	"github.com/ptourne/sistemas-distribuidos-1/common/logger"
)

const HEATBEAT_INTERVAL = 1 * time.Second // ToDo: ajustar

func SendHeartbeat(id string, addr string, log *logger.ConsoleLogger) {
	var conn net.Conn
	var err error
	udpAddr, err := net.ResolveUDPAddr("udp", addr)

	if err != nil {
		log.Errorf("Failed to resolve UDP address: %v", err)
		return
	}
	for {
		conn, err = net.DialUDP("udp", nil, udpAddr)
		if err != nil {
			//log.Errorf("Failed to connect to monitor: %v", err)
			continue
		}
		break

	}
	defer conn.Close()
	log.Infof("Sending heartbeat with ID: %s to %s", id, addr)
	for {
		msg := []byte(id)
		msgLen := len(msg)

		if msgLen > 255 {
			log.Fatalf("ID too long: %d bytes", msgLen)
		}

		packet := append([]byte{byte(msgLen)}, msg...)

		totalSent := 0

		for totalSent < len(packet) {
			sent, err := conn.Write(packet[totalSent:])
			if err != nil {
				log.Errorf("Failed to send heartbeat: %v", err)
			}
			totalSent += sent
		}

		time.Sleep(HEATBEAT_INTERVAL)
	}
}
