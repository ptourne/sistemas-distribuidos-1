package common

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
)

type TypeMsg int

const (
	FileName TypeMsg = iota
	FinishFile 
	FileData
	AllFilesSent
)


func WriteFull(writer io.Writer, buf []byte, n int) error {
	sent := 0
	for sent < n {
		m, err := writer.Write(buf[sent:n])
		if err != nil {
			return fmt.Errorf("error escribiendo: %v", err)
		}
		sent += m
	}
	return nil
}

func WriteProtocolTypeMsg(conn net.Conn, buf []byte, n int, typeMsg TypeMsg) error {
	return writeProtocol(conn, buf, n, int(typeMsg))
}

func WriteProtocolTypeRow(conn net.Conn, buf []byte, n int, typeRow TypeRow) error {
	return writeProtocol(conn, buf, n, int(typeRow))
}

func writeProtocol(conn net.Conn, buf []byte, n int, typeMsg int) error {
	sizeBuf := make([]byte, 8)
	binary.BigEndian.PutUint32(sizeBuf, uint32(n))
	binary.BigEndian.PutUint32(sizeBuf[4:], uint32(typeMsg))
	WriteFull(conn, sizeBuf, len(sizeBuf))
	WriteFull(conn, buf, n)
	err := waitAck(conn)
	if err != nil {
		return fmt.Errorf("error esperando ACK: %v", err)
	}
	return nil
}

func SendAck(conn net.Conn) error {
	bufAck := []byte("ACK")
	err := WriteFull(conn, bufAck, len(bufAck))
	return err
}


func waitAck(conn net.Conn) error {
	ackBuf := make([]byte, 3)
	_, err := io.ReadFull(conn, ackBuf)
	if err != nil {
		return fmt.Errorf("error recibiendo ACK: %v", err)
		
	}
	if string(ackBuf) != "ACK" {
		return fmt.Errorf("ACK inválido: %s", string(ackBuf))
	}
	return nil
}