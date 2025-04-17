package middleware

import (
	"fmt"
	"time"

	"github.com/ptourne/sistemas-distribuidos-1/common"
)

//ver de declare, tratr de devovler un struct, ghacer read/write
type MiddlewareCola interface {
	CreateReadQueue(readExchangeName string, readQueueName string) error
	CreateWriteQueue(writeExchangeName string) error
	Read(timeout *time.Timer) (*common.Row, error)
	Write(row *common.Row) error
	Close() error 
}

func NewMiddleware(tipo string) (MiddlewareCola, error) {
	if tipo == "rabbitmq" {
		return NewMiddlewareRabbitmq()
	}
	// podrías tener más opciones
	return nil, fmt.Errorf("tipo de middleware no soportado")
}