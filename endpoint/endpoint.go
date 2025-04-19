package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"

	"github.com/ptourne/sistemas-distribuidos-1/common"
	"github.com/ptourne/sistemas-distribuidos-1/middleware"
)
const MIDDLEWARE = "rabbitmq"


type Endpoint struct {
	Running bool
	listener  net.Listener
	clientsConn  map[string]net.Conn
}

func NewEndpoint() (*Endpoint, error) {
	ENDPOINT_PORT := os.Getenv("ENDPOINT_PORT")
	log.Infof("endpoint port: %s", ENDPOINT_PORT)
	port := ":" + ENDPOINT_PORT
	listener, err := net.Listen("tcp", port)
	if err != nil {
		return nil, err
	}

	endpoint := &Endpoint{
		Running: true,
		listener:  listener,
		clientsConn:  make(map[string]net.Conn),
	}
	return endpoint, nil
}

func (e *Endpoint) Run() error{
	middlewareChan, err := middleware.NewRabbitmq[[]byte]()
	if err != nil {
		return fmt.Errorf("failed to create middleware connection: %v", err)
	}
	log.Infof("Connected to middleware: %s", MIDDLEWARE)

	defer middlewareChan.Close()

	for e.Running {
		conn, ip, err := e.acceptNewConnection()
		if err != nil {
			if !e.Running {
				log.Infof("accepted connection fail for quitting")
				return nil
			}
			log.Errorf("accept_connections, error: %v", err)
			continue
		}
		// s.wg.Add(1)
		// go s.handleClientConnection(conn, ip)
		err = e.ReceiveFilesFromClient(conn, ip, middlewareChan)
		if err != nil {
			log.Errorf("error recibiendo archivos: %v", err)
			continue
		}
		// s.canRevealWinners()
	}
	// s.wg.Wait()
	return nil
}

func (s *Endpoint) acceptNewConnection() (net.Conn, string, error) {
	log.Infof("accepting connections")
	conn, err := s.listener.Accept()
	if err != nil {
		return nil, "", err
	}
	remoteAddr := conn.RemoteAddr().String()
	log.Infof("accept connection with ip: %s", remoteAddr)
	return conn, remoteAddr, nil
}

func (e *Endpoint) ReceiveFilesFromClient(conn net.Conn, ip string, middlewareChan middleware.MiddlewareCola[[]byte]) error{
	fileBytes := "file_bytes"
	fileBytesSender, err := middlewareChan.WriteTo(fileBytes)
	if err != nil {
		return fmt.Errorf("failed to create write queue %s: %v", fileBytes, err)
	}
	defer fileBytesSender.Close()

	e.clientsConn[ip] = conn
	defer func() {
		err := conn.Close()
		if err != nil {
			log.Errorf("Error closing connection: %s", err)
		}
		delete(e.clientsConn, ip)
		log.Infof("Connection closed with ip: %s", ip)
	}()

	log.Infof("Receiving files")
	OuterLoop:
	for {
		// Leer los primeros 8 bytes (tamaño y type)
		sizeBuf := make([]byte, 8)
		_, err := io.ReadFull(conn, sizeBuf)
		if err != nil {
			log.Infof("Error leyendo tamaño: %v", err)
			break
		}

		packetSize := binary.BigEndian.Uint32(sizeBuf[0:4])
		packetType := common.TypeMsg(binary.BigEndian.Uint32(sizeBuf[4:8]))
		dataBuf := make([]byte, packetSize)
		_, err = io.ReadFull(conn, dataBuf)
		if err != nil {
			log.Errorf("Error leyendo datos del paquete: %v", err)
			break
		}
		data := string(dataBuf)

		switch packetType {
		case common.FileName:
			log.Infof("Recibido FILE %s", data[5:])
		case common.FinishFile:
			log.Infof("Recibido FINISH %s", data[7:])
		case common.FileData:
			// err = common.WriteFull(sender, dataBuf, len(dataBuf))
		case common.AllFilesSent:
			log.Infof("Recibido ALL FILES SENT")
			bufAck := []byte("ACK")
			common.WriteFull(conn, bufAck, len(bufAck))
			break OuterLoop
		}
		typeDataBuf := append(sizeBuf[4:8], dataBuf...)
		err = fileBytesSender.Send(&typeDataBuf)
		if err != nil {
			log.Errorf("Error escribiendo al archivo: %v", err)
			break OuterLoop
		}
		bufAck := []byte("ACK")
		common.WriteFull(conn, bufAck, len(bufAck))

	}
	return nil
}


func (e *Endpoint) StopEndpoint() {
	log.Infof("Stopping endpoint")
	e.Running = false
	for _, conn := range e.clientsConn {
		err := conn.Close()
		if err != nil {
			log.Errorf("Error closing connection: %s", err)
		}
	}
	log.Infof("Endpoint stopped")
}
