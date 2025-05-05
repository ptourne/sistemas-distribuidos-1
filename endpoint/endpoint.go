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

	"github.com/ptourne/sistemas-distribuidos-1/common"
	"github.com/ptourne/sistemas-distribuidos-1/common/logger"
	"github.com/ptourne/sistemas-distribuidos-1/common/model"
	"github.com/ptourne/sistemas-distribuidos-1/middleware/middleware"
	"github.com/ptourne/sistemas-distribuidos-1/middleware/middleware/rabbitmq"
)

const MIDDLEWARE = "rabbitmq"

type Endpoint struct {
	Running         bool
	listener        net.Listener
	lockClientsConn sync.Mutex
	clientsConn     map[string]struct {
		conn   net.Conn
		output chan middleware.Envelope[*model.Row]
	}
	wg sync.WaitGroup
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
		Running:  true,
		listener: listener,
		clientsConn: make(map[string]struct {
			conn   net.Conn
			output chan middleware.Envelope[*model.Row]
		}),
		wg: sync.WaitGroup{},
	}
	return endpoint, nil
}

func (e *Endpoint) Run() error {
	connector, err := rabbitmq.Connector()
	if err != nil {
		log.Fatalf("Failed to connect to middleware: %s", err)
	}
	middlewareLogger := logger.NewConsoleLogger("coordinator", logger.Info)

	middlewareChanByte := rabbitmq.NewMiddleware[*common.PackageFile](connector, middlewareLogger)
	log.Infof("Connected to middleware: %s", MIDDLEWARE)
	defer middlewareChanByte.Close()

	middlewareChanRow := rabbitmq.NewMiddleware[*model.Row](connector, middlewareLogger)
	log.Infof("Connected to middleware: %s", MIDDLEWARE)
	defer middlewareChanRow.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		allQuerysToEndpointName := "all_querys_to_endpoint"
		receiverAllQuerysToEndpoint, err := middlewareChanRow.ConsumeFrom(allQuerysToEndpointName, allQuerysToEndpointName, 1, 1)
		if err != nil {
			log.Errorf("failed to create read queue %s: %v", allQuerysToEndpointName, err)
		}
		defer receiverAllQuerysToEndpoint.Close()
		for {
			envelope, err := receiverAllQuerysToEndpoint.Next(ctx)
			if err != nil {
				if err.Error() == "read channel was closed" {
					log.Infof("Channel closed: %v", allQuerysToEndpointName)
					break
				}
				if err.Error() == "timeout reached while waiting for message or ctx canceled" {
					log.Infof("Timeout reached while waiting for message or ctx canceled")
					break
				}
				log.Errorf("Error reading from middleware: %v", err)
				continue
			}
			cid := envelope.Cid()
			e.lockClientsConn.Lock()
			structCid, exists := e.clientsConn[cid]
			e.lockClientsConn.Unlock()
			outputCid := structCid.output
			if !exists {
				log.Errorf("Cid not found: %v", cid)
				break
			}
			switch envelope.Type() {
			case middleware.EOF:
				log.Infof("cid %s finished receiving", cid)
				e.lockClientsConn.Lock()
				delete(e.clientsConn, envelope.Cid())
				e.lockClientsConn.Unlock()
			case middleware.Prune:
				err = envelope.Ack(false)
				if err != nil {
					log.Errorf("failed to ack message in endpoint %s", err)
				}
				continue
			}
			outputCid <- envelope
		}
	}()

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
		go e.handleClient(conn, ip, middlewareChanByte, cid)
	}
	e.wg.Wait()
	return nil
}

func (e *Endpoint) handleClient(conn net.Conn, ip string, middlewareChanByte middleware.Connection[*common.PackageFile], cid string) {
	defer e.wg.Done()
	structCid := struct {
		conn   net.Conn
		output chan middleware.Envelope[*model.Row]
	}{conn: conn, output: make(chan middleware.Envelope[*model.Row])}
	e.lockClientsConn.Lock()
	e.clientsConn[cid] = structCid
	e.lockClientsConn.Unlock()
	err := e.ReceiveFilesFromClient(conn, ip, middlewareChanByte, cid)
	if err != nil {
		log.Errorf("error recibiendo archivos: %v", err)
	}
	err = e.ReceiveAndSendQuerysResults(conn, ip, structCid.output, cid)
	if err != nil {
		log.Errorf("error recibiendo o enviando querys: %v", err)
	}
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

func (e *Endpoint) ReceiveAndSendQuerysResults(conn net.Conn, ip string, output chan middleware.Envelope[*model.Row], cid string) error {
	log.Infof("Receiving and sending querys results to client %s", cid)
	var err error
OuterLoop:
	for {
		envelope := <-output
		if envelope.Cid() != cid {
			log.Errorf("Received message from wrong cid: %s", envelope.Cid())
			continue
		}
		switch envelope.Type() {
		case middleware.EOF:
			log.Infof("No more querys")
			bufAck := []byte("FinishQuerys")
			err = common.WriteProtocolTypeRow(conn, bufAck, len(bufAck), model.FinishQuerys)
			if err != nil {
				log.Errorf("Failed to send message: %v", err)
			}
			err = envelope.Ack(false)
			if err != nil {
				return fmt.Errorf("failed to ack message in endpoint %s", err)
			}
			break OuterLoop
		case middleware.Prune:
			err = envelope.Ack(false)
			if err != nil {
				return fmt.Errorf("failed to ack message in endpoint %s", err)
			}
			continue
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
			return fmt.Errorf("failed to ack message in endpoint %s", err)
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
			// err = fileBytesSender.SendEOF(cid)
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
	for _, structCid := range e.clientsConn {
		err := structCid.conn.Close()
		if err != nil {
			log.Errorf("Error closing connection: %s", err)
		}
		close(structCid.output)
	}
	e.clientsConn = make(map[string]struct {
		conn   net.Conn
		output chan middleware.Envelope[*model.Row]
	})
	e.lockClientsConn.Unlock()
	log.Infof("Endpoint stopped")
}
