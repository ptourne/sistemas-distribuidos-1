package middleware

import (
	"time"
)

// ver de declare, tratr de devovler un struct, ghacer read/write
type MiddlewareCola[T any] interface {
	ConsumeFrom(sourceName string, groupName string) (Receiver[T], error)
	SuscribeTo(sourceName string) (Receiver[T], error)
	CreateWriteQueue(writeExchangeName string) (Sender[T], error)
	Close() error
}

type Receiver[T any] interface {
	Next(timeout *time.Timer) (Envelope[T], error)
	Close() error
}

type Envelope[T any] interface {
	Msg() T
	Ack(multiple bool) error
}

type Sender[T any] interface {
	Send(row *T) error
	Close() error
}
