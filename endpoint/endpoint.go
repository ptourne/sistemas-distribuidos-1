package main

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"time"

	"github.com/ptourne/sistemas-distribuidos-1/common"
	"github.com/ptourne/sistemas-distribuidos-1/middleware"
)

const MIDDLEWARE = "rabbitmq"

type Endpoint struct {
	Running     bool
	listener    net.Listener
	clientsConn map[string]net.Conn
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
		Running:     true,
		listener:    listener,
		clientsConn: make(map[string]net.Conn),
	}
	return endpoint, nil
}

func (e *Endpoint) Run() error{
	middlewareChanByte, err := middleware.NewRabbitmq[[]byte]()
	if err != nil {
		return fmt.Errorf("failed to create middleware connection: %v", err)
	}
	log.Infof("Connected to middleware: %s", MIDDLEWARE)
	defer middlewareChanByte.Close()

	middlewareChanRow, err := middleware.NewRabbitmq[common.Row]()
	if err != nil {
		return fmt.Errorf("failed to create middleware connection: %v", err)
	}
	log.Infof("Connected to middleware: %s", MIDDLEWARE)
	defer middlewareChanRow.Close()

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
		err = e.handleClient(conn, ip, middlewareChanByte, middlewareChanRow)
		if err != nil {
			log.Errorf("error handling client: %v", err)
			continue
		}
	}
	// s.wg.Wait()
	return nil
}

func (e *Endpoint) handleClient(conn net.Conn, ip string, middlewareChanByte middleware.MiddlewareCola[[]byte], middlewareChanRow middleware.MiddlewareCola[common.Row]) error {
	err := e.ReceiveFilesFromClient(conn, ip, middlewareChanByte)
	if err != nil {
		return fmt.Errorf("error recibiendo archivos: %v", err)
	}
	err = e.ReceiveAndSendQuerysResults(conn, ip, middlewareChanRow)
	if err != nil {
		return fmt.Errorf("error recibiendo o enviando querys: %v", err)
	}
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

func (e *Endpoint) ReceiveAndSendQuerysResults(conn net.Conn, ip string, middlewareChan middleware.MiddlewareCola[common.Row]) error{
	allQuerysToEndpointName :="all_querys_to_endpoint"
	receiverAllQuerysToEndpoint, err := middlewareChan.ConsumeFrom(allQuerysToEndpointName, allQuerysToEndpointName)
	if err != nil {
		return fmt.Errorf("failed to create read queue %s: %v", allQuerysToEndpointName, err)
	}
	defer receiverAllQuerysToEndpoint.Close()

	timer := time.NewTimer(time.Minute * 100)
	for {
		envelope, ok, err := receiverAllQuerysToEndpoint.Next(timer)
		if err != nil {
			if err.Error() == "timeout reached while waiting for message" {
				log.Infof("Timeout reached while waiting for message")
				break
			} else {
				log.Errorf("Failed to read message: %v", err)
				continue
			}
		}
		if !ok {
			log.Infof("No more querys")
			bufAck := []byte("FinishQuerys")
			err = common.WriteProtocolTypeRow(conn, bufAck, len(bufAck), common.FinishQuerys)
			if err != nil {
				log.Errorf("Failed to send message: %v", err)
			}
			break
		}
		receivedMovie := envelope.Msg()
		var bufAck []byte
		switch receivedMovie.Type {
		case common.QueryName:
			bufAck = []byte(receivedMovie.Strings["type"])
		case common.QueryRow:
			bufAck, err = json.Marshal(receivedMovie)
			if err != nil {
				return fmt.Errorf("error in marshal row %v",err)
			}
		}

		err = common.WriteProtocolTypeRow(conn, bufAck, len(bufAck), receivedMovie.Type)
		if err != nil {
			log.Errorf("Failed to send message: %v", err)
			continue
		}
		err = envelope.Ack(false)
		if err != nil {
			return fmt.Errorf("failed to ack message %s",err)
		}
		timer.Reset(time.Minute * 10)
	}
	timer.Stop()
	return nil
}

func (e *Endpoint) ReceiveFilesFromClient(conn net.Conn, ip string, middlewareChan middleware.MiddlewareCola[[]byte]) error{
	fileBytes := "file_bytes"
	fileBytesSender, err := middlewareChan.WriteTo(fileBytes, []string{"file_bytes"})
	if err != nil {
		return fmt.Errorf("failed to create write queue %s: %v", fileBytes, err)
	}
	defer fileBytesSender.Close()

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
			log.Infof("Recibido FILE %s", data)

		case common.FinishFile:
			log.Infof("Recibido FINISH %s", data)

		case common.FileData:

		case common.AllFilesSent:
			log.Infof("Recibido ALL FILES SENT")
			typeDataBuf := append(sizeBuf[4:8], dataBuf...)
			err = fileBytesSender.Send(&typeDataBuf)
			if err != nil {
				log.Errorf("Error escribiendo al archivo: %v", err)
				break OuterLoop
			}
			err = common.SendAck(conn)
			if err != nil {
				log.Errorf("Error enviando ACK: %v", err)
			}
			break OuterLoop
		}
		typeDataBuf := append(sizeBuf[4:8], dataBuf...)
		err = fileBytesSender.Send(&typeDataBuf)
		if err != nil {
			log.Errorf("Error escribiendo al archivo: %v", err)
			break OuterLoop
		}
		err = common.SendAck(conn)
		if err != nil {
			log.Errorf("Error enviando ACK: %v", err)
			break OuterLoop
		}
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
