package main

// import (
// 	"bytes"
// 	"encoding/csv"
// 	"fmt"
// 	"io"
// 	"net"
// 	"os"
// )

// func (c *Client) SendFiles() {
// 	log.Infof("Sending files")
// 	writer := csv.NewWriter(c.conn) // escribe en el socket como si fuera un archivo

// 	sendRecords("movies_metadata.csv", 24, writer, c.conn)
// 	sendRecords("credits.csv", 3, writer, c.conn)
// 	sendRecords("ratings.csv", 4, writer, c.conn)

// 	writer.Write([]string{"#FINISH"})
// 	log.Infof("Files sent")
// 	response := make([]byte, 1024)
// 	_, err := c.conn.Read(response)
// 	if err != nil {
// 		log.Errorf("Error reading response: %v", err)
// 		return
// 	}
// 	log.Infof("Response received")
// }

// func sendRecords(fileName string, columns int, writer *csv.Writer, conn net.Conn) error {
// 	filePath:= "/datasets/" + fileName
// 	file, err := os.Open(filePath)
// 	if err != nil {
// 		return fmt.Errorf("error opening file %s: %v", fileName, err)
// 	}
// 	defer file.Close()
// 	reader := csv.NewReader(file)

// 	writer.Write([]string{"#FILE", fileName})
// 	_, err = reader.Read()
// 	if err != nil {
// 		return fmt.Errorf("error reading header: %v", err)
// 	}

// 	line := 0
// 	log.Debugf("Starting CSV processing")
// 	for {
// 		line++
// 		if line%1000 == 0 {
// 			log.Infof("Processed %d lines", line)
// 		}
// 		record, err := reader.Read()
// 		if err != nil {
// 			if err == io.EOF {
// 				break
// 			}
// 			log.Errorf("Error reading CSV line: %v", err)
// 			continue
// 		}
// 		if len(record) < columns {
// 			continue
// 		}
// 		var buf bytes.Buffer
// 		csvWriter := csv.NewWriter(&buf)
// 		csvWriter.Write(record)
// 		csvWriter.Flush()

// 		if err := safeWrite(conn, buf.Bytes()); err != nil {
// 			log.Errorf("Failed to send record: %v", err)
// 		}
// 	}
// 	log.Debugf("CSV %s sent", fileName)
// 	return nil
// }

// func safeWrite(conn net.Conn, data []byte) error {
// 	total := 0
// 	for total < len(data) {
// 		n, err := conn.Write(data[total:])
// 		if err != nil {
// 			return err
// 		}
// 		total += n
// 	}
// 	return nil
// }

// func (c *Client) StopClient() {
// 	log.Infof("Stopping client")
// 	c.Running = false
// 	err := c.conn.Close()
// 	if err != nil {
// 		log.Errorf("Error closing connection: %s", err)
// 	}
// 	log.Infof("Client stopped")
// }