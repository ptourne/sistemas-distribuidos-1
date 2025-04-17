package middleware

import (
	"fmt"
	"time"
)

//ver de declare, tratr de devovler un struct, ghacer read/write
type MiddlewareCola[T any] interface {
	CreateReadQueue (readExchangeName string, readQueueName string) (Receiver[T], error)
	CreateWriteQueue(writeExchangeName string) (Sender[T],error)
	Close() error 
}

type Receiver[T any] interface {
	Next(timeout *time.Timer) (Envelope[T], error)
}

type Envelope[T any] interface {
	Msg() T
	Ack(multiple bool) error
}

type Sender[T any] interface {
	Send(row *T) error
}


func NewMiddleware[T any](tipo string) (MiddlewareCola[T], error) {
	if tipo == "rabbitmq" {
		return NewMiddlewareRabbitmq[T]()
	}
	// podrías tener más opciones
	return nil, fmt.Errorf("tipo de middleware no soportado")
}

