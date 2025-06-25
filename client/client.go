package main

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"

	"github.com/ptourne/sistemas-distribuidos-1/common"
	"github.com/ptourne/sistemas-distribuidos-1/common/model"
)

const CHUNK_SIZE = 1024

func (c *Client) Reconnect(addr string) error {
	log.Infof("Intentando reconectar...")
	if c.conn != nil {
		c.conn.Close()
	}

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return fmt.Errorf("error al reconectar: %v", err)
	}
	c.conn = conn
	log.Infof("Reconectado exitosamente")
	return nil
}

func (c *Client) Run() error {
	log.Infof("Sending files")
	err := c.SendFiles()
	if err != nil {
		return fmt.Errorf("error sending files: %v", err)
	}

	err = c.ReceivingQuerysResults()
	if err != nil {
		return fmt.Errorf("error receiving querys results: %v", err)
	}

	log.Infof("Finished ALL")

	return nil
}

func (c *Client) ReceivingQuerysResults() error {
	log.Infof("Receiving querys")
	var queryType string
OuterLoop:
	for {
		// Leer los primeros 8 bytes (tamaño y type)
		sizeBuf := make([]byte, 8)
		_, err := io.ReadFull(c.conn, sizeBuf)
		if err != nil {
			log.Infof("Error leyendo tamaño: %v", err)
			return err
		}

		packetSize := binary.BigEndian.Uint32(sizeBuf[0:4])
		packetType := model.TypeRow(binary.BigEndian.Uint32(sizeBuf[4:8]))
		dataBuf := make([]byte, packetSize)
		_, err = io.ReadFull(c.conn, dataBuf)
		if err != nil {
			log.Errorf("Error leyendo datos del paquete: %v", err)
			return err
		}

		switch packetType {
		case model.QueryName:
			data := string(dataBuf)
			queryType = data
			log.Infof("Query: %s", data)

		case model.QueryRow:
			var movie model.Row
			err = json.Unmarshal(dataBuf, &movie)
			if err != nil {
				log.Errorf("Error unmarshaling data: %v", err)
				return err
			}
			switch queryType {
			case "Q1":
				printRowQ1(movie)
			case "Q2":
				printRowQ2(movie)
			case "Q4":
				printRowQx(movie)
			case "Q3":
				printRowQ3(movie)
			case "Q5":
				printRowQx(movie)
			default:
				log.Infof("Query no soportada: %s", queryType)
				return fmt.Errorf("query no soportada: %s", queryType)
			}

		case model.FinishQuerys:
			log.Infof("Recibido FINISH QUERYS")
			err = common.SendAck(c.conn)
			if err != nil {
				log.Errorf("Error enviando ACK: %v", err)
			}
			break OuterLoop
		}
		err = common.SendAck(c.conn)
		if err != nil {
			log.Errorf("Error enviando ACK: %v", err)
			break OuterLoop
		}
	}
	return nil
}

func (c *Client) SendFiles() error {
	filesNames := []string{"movies_metadata", "credits"} //, "ratings"
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
	err := common.WriteProtocolTypePackage(c.conn, bufFinish, len(bufFinish), common.AllFilesSent)
	if err != nil {
		return err
	}
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
	err = common.WriteProtocolTypePackage(c.conn, bufFile, len(bufFile), common.FileName)
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

		common.WriteProtocolTypePackage(c.conn, buf, n, common.FileData)
		if err == io.ErrUnexpectedEOF {
			break
		}
	}
	bufFinish := []byte(fileName)
	return common.WriteProtocolTypePackage(c.conn, bufFinish, len(bufFinish), common.FinishFile)
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

func printRowQ1(row model.Row) {
	log.Infof("movieId:%s, movieTitle:%s, movieGenres:%v, movieProductionCountries:%v", row.Strings["movieID"], row.Strings["title"], row.Arrays["genres"], row.Arrays["production_countries"])
}

func printRowQ2(row model.Row) {
	log.Infof("country:%s, budget:%d", row.Strings["country"], row.Numerics["budget_sum"])
}

func printRowQ3(row model.Row) {
	log.Infof("movieID:%s, title:%s, avg_rating:%f", row.Strings["movieID"], row.Strings["title"], row.Floats["avg_rating"])
}

func printRowQx(row model.Row) {
	log.Infof("Row: %+v", row)
}
