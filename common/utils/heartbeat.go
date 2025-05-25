package utils

import (
	"fmt"
	"io"
	"net"
	"strings"
	"time"

	"github.com/ptourne/sistemas-distribuidos-1/common/logger"
)

const HEATBEAT_INTERVAL = 1 * time.Second // ToDo: ajustar
const HEADER_SIZE = 1

func SendHeartbeat(id string, addrs string, log *logger.ConsoleLogger) {
	sendHeartbeat(id, addrs, log, false, nil)
}

func SendStoppableHeartbeat(id string, addrs string, log *logger.ConsoleLogger, stopChan <-chan bool) {
	sendHeartbeat(id, addrs, log, false, stopChan)
}

func SendExit(idClient string, addrs string, log *logger.ConsoleLogger) {
	localAddr, _ := net.ResolveUDPAddr("udp", ":0")
	conn, err := net.ListenUDP("udp", localAddr)
	if err != nil {
		log.Fatalf("Failed to open UDP socket: %v", err)
	}
	defer conn.Close()

	for {

		sendHeartbeat(idClient, addrs, log, true, nil)
		log.Infof("Sent exit heartbeat to monitors")

		msg, err := ReceiveMessage(conn)
		if err != nil {
			continue
		}

		if strings.HasPrefix(msg, "ACK") {
			log.Infof("Received ACK")
			break
		}

	}
}

func WriteToConn(addr string, log *logger.ConsoleLogger, packet []byte, conn net.Conn) {
	totalSent := 0

	for totalSent < len(packet) {
		sent, err := conn.Write(packet[totalSent:])
		if err != nil {
			log.Errorf("Failed to send heartbeat to %s: %v", addr, err)
			continue
		}
		totalSent += sent
	}
}

func getUdpAddrs(addrs string, log *logger.ConsoleLogger) []*net.UDPAddr {
	addresses := strings.Split(addrs, ",")

	udpAddrs := []*net.UDPAddr{}
	for _, addr := range addresses {
		for { // ToDo: timeout o max retries
			udpAddr, err := net.ResolveUDPAddr("udp", strings.TrimSpace(addr))
			if err != nil {
				log.Errorf("Failed to resolve UDP address '%s': %v", addr, err)
			} else {

				udpAddrs = append(udpAddrs, udpAddr)
				break
			}
		}
	}
	return udpAddrs
}

func sendHeartbeat(id string, addrs string, log *logger.ConsoleLogger, exit bool, stopChan <-chan bool) {
	udpAddrs := getUdpAddrs(addrs, log)
	log.Infof("Sending heartbeat to monitors: %s", udpAddrs)
	for {
		select {
		case <-stopChan:
			log.Infof("Stopping heartbeat")
			return
		default:
			msg := []byte(id)
			if exit {
				msg = []byte(id + "|e")
			}
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

				WriteToConn(addr.String(), log, packet, conn)

				conn.Close()
			}

			time.Sleep(HEATBEAT_INTERVAL)
			if exit {
				break
			}
		}
	}
}

func ReceiveMessage(conn net.Conn) (string, error) {
	header, err := recvHeader(conn, HEADER_SIZE)
	if err != nil {
		return "", fmt.Errorf("error receiving message: %w", err)
	}

	msgSize := uint8(header[0])

	buffer, err := recvMessage(msgSize, conn)
	if err != nil {
		return "", fmt.Errorf("error receiving message: %w", err)
	}

	return string(buffer), nil
}

func recvMessage(msgSize uint8, conn net.Conn) ([]byte, error) {
	buffer := make([]byte, msgSize)
	_, err := io.ReadFull(conn, buffer)
	if err != nil {
		return nil, err
	}
	return buffer, nil
}

func recvHeader(conn net.Conn, headerSize int) ([]byte, error) {
	header := make([]byte, headerSize)
	_, err := io.ReadFull(conn, header)
	if err != nil {
		return nil, err
	}
	return header, nil
}
