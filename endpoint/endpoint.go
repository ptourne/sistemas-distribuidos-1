package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"time"

	"github.com/ptourne/sistemas-distribuidos-1/common"
	"github.com/ptourne/sistemas-distribuidos-1/common/model"
	"github.com/ptourne/sistemas-distribuidos-1/middleware/middleware"
	"github.com/ptourne/sistemas-distribuidos-1/middleware/middleware/rabbitmq"
)

const MIDDLEWARE = "rabbitmq"

type Endpoint struct {
	Running         bool
	listener        net.Listener
	lockClientsConn sync.Mutex
	clientsConn     map[string]net.Conn
	wg              sync.WaitGroup
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
		wg:          sync.WaitGroup{},
	}
	return endpoint, nil
}

func (e *Endpoint) Run() error {
	connector, err := rabbitmq.Connector()
	if err != nil {
		log.Fatalf("Failed to connect to middleware: %s", err)
	}
	middlewareChanByte := rabbitmq.NewMiddleware[*common.PackageFile](connector)
	log.Infof("Connected to middleware: %s", MIDDLEWARE)
	defer middlewareChanByte.Close()

	middlewareChanRow := rabbitmq.NewMiddleware[*model.Row](connector)
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
		cid := GenerateRandomID()
		log.Infof("Accepted connection with id: %s", cid)
		e.wg.Add(1)
		go e.handleClient(conn, ip, middlewareChanByte, middlewareChanRow, cid)
	}
	e.wg.Wait()
	return nil
}

func (e *Endpoint) handleClient(conn net.Conn, ip string, middlewareChanByte middleware.Connection[*common.PackageFile], middlewareChanRow middleware.Connection[*model.Row], cid string) {
	e.lockClientsConn.Lock()
	e.clientsConn[cid] = conn
	e.lockClientsConn.Unlock()
	err := e.ReceiveFilesFromClient(conn, ip, middlewareChanByte, cid)
	if err != nil {
		log.Errorf("error recibiendo archivos: %v", err)
	}
	// err = e.ReceiveAndSendQuerysResults(conn, ip, middlewareChanRow, cid)
	// if err != nil {
	// 	log.Errorf("error recibiendo o enviando querys: %v", err)
	// }
	e.lockClientsConn.Lock()
	log.Infof("Closing connection with id: %s", cid)
	conn.Close()
	delete(e.clientsConn, cid)
	e.lockClientsConn.Unlock()
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

func (e *Endpoint) ReceiveAndSendQuerysResults(conn net.Conn, ip string, middlewareChan middleware.Connection[*model.Row], cid string) error {
	allQuerysToEndpointName := "all_querys_to_endpoint"
	receiverAllQuerysToEndpoint, err := middlewareChan.ConsumeFrom(allQuerysToEndpointName, allQuerysToEndpointName, 1, 1)
	if err != nil {
		return fmt.Errorf("failed to create read queue %s: %v", allQuerysToEndpointName, err)
	}
	defer receiverAllQuerysToEndpoint.Close()

	for {
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Minute)
		envelope, ok, err := receiverAllQuerysToEndpoint.Next(ctx)
		cancel()
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
			err = common.WriteProtocolTypeRow(conn, bufAck, len(bufAck), model.FinishQuerys)
			if err != nil {
				log.Errorf("Failed to send message: %v", err)
			}
			break
		}
		receivedMovie := envelope.Msg()
		var bufAck []byte
		switch receivedMovie.Type {
		case model.QueryName:
			bufAck = []byte(receivedMovie.Strings["type"])
		case model.QueryRow:
			bufAck, err = json.Marshal(receivedMovie)
			if err != nil {
				return fmt.Errorf("error in marshal row %v", err)
			}
		}

		err = common.WriteProtocolTypeRow(conn, bufAck, len(bufAck), receivedMovie.Type)
		if err != nil {
			log.Errorf("Failed to send message: %v", err)
			continue
		}
		err = envelope.Ack(false)
		if err != nil {
			return fmt.Errorf("failed to ack message %s", err)
		}
	}
	return nil
}

func (e *Endpoint) ReceiveFilesFromClient(conn net.Conn, ip string, middlewareChan middleware.Connection[*common.PackageFile], cid string) error {
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
		packetType := common.TypePackage(binary.BigEndian.Uint32(sizeBuf[4:8]))
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
			err = fileBytesSender.SendEOF(cid)
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
		msg := &common.PackageFile{
			PackageType: packetType,
			Buf: model.FileChunk{
				Bytes: []byte(data),
			},
		}
		err = fileBytesSender.Send(msg, cid)
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
	if e.listener != nil {
		err := e.listener.Close()
		if err != nil {
			log.Errorf("Error closing listener: %s", err)
		}
	}
	e.lockClientsConn.Lock()
	for _, conn := range e.clientsConn {
		err := conn.Close()
		if err != nil {
			log.Errorf("Error closing connection: %s", err)
		}
	}
	e.clientsConn = make(map[string]net.Conn)
	e.lockClientsConn.Unlock()
	log.Infof("Endpoint stopped")
}
