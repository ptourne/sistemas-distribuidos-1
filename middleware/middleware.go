package middleware

import (
	"time"
)

// ver de declare, tratr de devovler un struct, ghacer read/write
type MiddlewareCola[T any] interface {
	ConsumeFrom(sourceName string, groupName string) (Receiver[T], error)
	SuscribeTo(sourceName string) (Receiver[T], error)
	WriteTo(writeExchangeName string, subscribers []string) (Sender[T], error)
	Close() error
}

type Receiver[T any] interface {
	Next(timeout *time.Timer) (Envelope[T], bool, error)
	CountProducers() (int, error)
	Qos(int, int) error
	Close() error
	/* LimitUnacked(limit int) error
	NotifyBlocked()
	IsBlocked() bool
	NotifyClose()
	IsClosed() bool */
}

type Envelope[T any] interface {
	Msg() T
	Ack(multiple bool) error
	Nack(multiple bool) error
}

type Sender[T any] interface {
	Send(row *T) error
	Close() error
	/* LimitUnacked(limit int) error
	NotifyBlocked()
	IsBlocked() bool
	NotifyClose()
	IsClosed() bool */
}
