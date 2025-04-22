package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"

	"github.com/ptourne/sistemas-distribuidos-1/common"
)


const CHUNK_SIZE = 1024 

func (c *Client) SendFiles() error{
	filesNames :=[]string{"movies_metadata","ratings","credits"}
	log.Infof("Sending files")
	for _, fileName := range filesNames {
		log.Infof("sending file: %s", fileName)
		err := sendFile(c, fileName)
		if c.Running {
			if err != nil {
				return fmt.Errorf("error sending %s: %v", fileName, err)
			}
			log.Infof("Archivo %s enviado exitosamente", fileName)
		}
	}
	bufFinish := []byte("ALL FILES SENT")
	writeProtocol(c, bufFinish, len(bufFinish), common.AllFilesSent)
	log.Infof("Files sent")
	return nil
}

func sendFile(c *Client, fileName string) error {
	filePath := "/datasets/" + fileName + ".csv"
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("error abriendo el archivo %s: %v", fileName, err)
	}
	defer file.Close()

	bufFile := []byte(fileName)
	err = writeProtocol(c, bufFile, len(bufFile), common.FileName)
	if err != nil {
		return fmt.Errorf("error enviando nombre de archivo: %v", err)
	}

	reader := bufio.NewReader(file)
	buf := make([]byte, CHUNK_SIZE)

	for c.Running {
		n, err := io.ReadFull(reader, buf)
		if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF { 
			return fmt.Errorf("error leyendo archivo: %v", err)
		}

		if n == 0 {
			break 
		}

		writeProtocol(c, buf, n, common.FileData)
		if err == io.ErrUnexpectedEOF{
			break 
		}
	}
	bufFinish := []byte(fileName)
	writeProtocol(c, bufFinish, len(bufFinish), common.FinishFile)
	return nil
}

func writeProtocol(c *Client, buf []byte, n int, typeMsg common.TypeMsg) error {
	sizeBuf := make([]byte, 8)
	binary.BigEndian.PutUint32(sizeBuf, uint32(n))
	binary.BigEndian.PutUint32(sizeBuf[4:], uint32(typeMsg))
	common.WriteFull(c.conn, sizeBuf, len(sizeBuf))
	common.WriteFull(c.conn, buf, n)
	err := waitAck(c)
	if err != nil {
		return fmt.Errorf("error esperando ACK: %v", err)
	}
	return nil
}

func waitAck(c *Client) error {
	ackBuf := make([]byte, 3)
	_, err := io.ReadFull(c.conn, ackBuf)
	if err != nil {
		return fmt.Errorf("error recibiendo ACK: %v", err)
		
	}
	if string(ackBuf) != "ACK" {
		return fmt.Errorf("ACK inválido: %s", string(ackBuf))
	}
	return nil
}

func (c *Client) StopClient() {
	log.Infof("Stopping client")
	c.Running = false
	err := c.conn.Close()
	if err != nil {
		log.Errorf("Error closing connection: %s", err)
	}
	log.Infof("Client stopped")
}