package middleware

import (
	"fmt"
	"time"

	"github.com/ptourne/sistemas-distribuidos-1/common"
)

//ver de declare, tratr de devovler un struct, ghacer read/write
type MiddlewareCola interface {
	CreateReadQueue(readExchangeName string, readQueueName string) (Receiver, error)
	CreateWriteQueue(writeExchangeName string) (Sender,error)
	// Read(readExchangeName string, readQueueName string, timeout *time.Timer) (*common.Row, error)
	// Write(writeExchangeName string,row *common.Row) error
	Close() error 
}

type Receiver interface {
	Next(timeout *time.Timer) (*common.Row, error)
}

type Sender interface {
	Send(row *common.Row) error
}

func NewMiddleware(tipo string) (MiddlewareCola, error) {
	if tipo == "rabbitmq" {
		return NewMiddlewareRabbitmq()
	}
	// podrías tener más opciones
	return nil, fmt.Errorf("tipo de middleware no soportado")
}

