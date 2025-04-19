package common

import (
	"fmt"
	"io"
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