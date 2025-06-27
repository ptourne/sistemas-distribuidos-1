package utils

import (
	"context"
	"fmt"
	"io"
	"net"
	"strings"
	"time"

	"github.com/ptourne/sistemas-distribuidos-1/common/logger"
)

const HEATBEAT_INTERVAL = 100 * time.Millisecond // ToDo: ajustar
const HEADER_SIZE = 1
const UDP_TIMEOUT = 1 * time.Second
const READ_TIMEOUT = 100 * time.Millisecond

func SendHeartbeat(id string, addrs string, log *logger.ConsoleLogger, ctx context.Context) {
	sendHeartbeat(id, addrs, log, false, ctx)
}

func SendExit(idClient string, addrs string, log *logger.ConsoleLogger) {
	localAddr, _ := net.ResolveUDPAddr("udp", ":0") // puerto disponible random
	conn, err := net.ListenUDP("udp", localAddr)
	if err != nil {
		log.Fatalf("Failed to open UDP socket: %v", err)
	}
	defer conn.Close()

	ctx := context.Background()

	for {

		sendHeartbeatWithConn(idClient, addrs, log, true, ctx, conn, HEATBEAT_INTERVAL)
		log.Infof("Sent exit heartbeat to monitors")

		conn.SetReadDeadline(time.Now().Add(UDP_TIMEOUT))

		msg, err := ReceiveUDPMessage(conn)
		if err != nil {
			log.Warnf("Failed to receive message: %v", err)
			continue
		}

		if strings.HasPrefix(msg, "ACK") {
			log.Infof("Received ACK")
			break
		}

	}
}

func WriteToConn(addr string, packet []byte, conn net.Conn) error {
	totalSent := 0

	conn.SetWriteDeadline(time.Now().Add(1 * time.Second))

	for totalSent < len(packet) {
		sent, err := conn.Write(packet[totalSent:])
		if err != nil {
			conn.SetWriteDeadline(time.Time{})
			return err
		}
		totalSent += sent
	}
	conn.SetWriteDeadline(time.Time{})
	return nil
}

func WriteUDP(addr *net.UDPAddr, log *logger.ConsoleLogger, packet []byte, conn *net.UDPConn) {
	totalSent := 0

	for totalSent < len(packet) {
		sent, err := conn.WriteToUDP(packet[totalSent:], addr)
		if err != nil {
			log.Errorf("Failed to write to %s: %v", addr, err)
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
			if err == nil {

				udpAddrs = append(udpAddrs, udpAddr)
				break
			}
		}
	}
	return udpAddrs
}

func sendHeartbeatWithConn(id string, addrs string, log *logger.ConsoleLogger, exit bool, ctx context.Context, conn *net.UDPConn, heartbeat_interval time.Duration) {
	udpAddrs := getUdpAddrs(addrs, log)
	for {
		select {
		case <-ctx.Done():
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
				WriteUDP(addr, log, packet, conn)
			}

			if exit {
				return
			}

			time.Sleep(heartbeat_interval)
		}
	}
}

func sendHeartbeat(id string, addrs string, log *logger.ConsoleLogger, exit bool, ctx context.Context) {

	conn, err := net.ListenUDP("udp", nil)
	if err != nil {
		log.Fatalf("Failed to open UDP socket: %v", err)
	}
	defer conn.Close()
	sendHeartbeatWithConn(id, addrs, log, exit, ctx, conn, HEATBEAT_INTERVAL)
}

func SendMonitorHeartbeat(id string, addrs string, log *logger.ConsoleLogger, exit bool, ctx context.Context, heartbeat_interval time.Duration) {

	conn, err := net.ListenUDP("udp", nil)
	if err != nil {
		log.Fatalf("Failed to open UDP socket: %v", err)
	}
	defer conn.Close()
	sendHeartbeatWithConn(id, addrs, log, exit, ctx, conn, heartbeat_interval)
}

func ReceiveUDPMessage(conn *net.UDPConn) (string, error) {
	buffer := make([]byte, 1024)
	n, _, err := conn.ReadFromUDP(buffer)
	if err != nil {
		return "", err
	}
	return string(buffer[:n]), nil
}

func ReceiveTCPMessage(conn net.Conn) (string, error) {
	if err := conn.SetReadDeadline(time.Now().Add(READ_TIMEOUT)); err != nil {
		return "", fmt.Errorf("could not set read deadline: %w", err)
	}

	header, err := recvHeader(conn, HEADER_SIZE)
	if err != nil {
		conn.SetWriteDeadline(time.Time{})
		return "", fmt.Errorf("error receiving message: %w", err)
	}

	msgSize := uint8(header[0])

	buffer, err := recvMessage(msgSize, conn)
	if err != nil {
		conn.SetWriteDeadline(time.Time{})
		return "", fmt.Errorf("error receiving message: %w", err)
	}

	_ = conn.SetReadDeadline(time.Time{})

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
