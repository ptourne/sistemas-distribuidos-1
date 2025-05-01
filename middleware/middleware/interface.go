package middleware

import (
	"time"

	"github.com/ptourne/sistemas-distribuidos-1/middleware/codec"
)

// ver de declare, tratr de devovler un struct, ghacer read/write
type Connection[T codec.Serializable] interface {
	ConsumeFrom(sourceName string, groupName string) (Receiver[T], error)
	ConsumeFromRK(sourceName string, groupName string, t string, routingkey string) (Receiver[T], error)
	WriteTo(writeExchangeName string, subscribers []string) (Sender[T], error)
	WriteToRK(writeExchangeName string, subscribers map[string][]string, t string) (Sender[T], error)
	Close() error
}

type Receiver[T codec.Serializable] interface {
	Next(timeout *time.Timer) (Envelope[T], bool, error)
	CountProducers() (int, error)
	Qos(int, int) error
	Close() error
}

type Envelope[T codec.Serializable] interface {
	Msg() T
	Ack(multiple bool) error
	Nack(multiple bool) error
}

type Sender[T codec.Serializable] interface {
	Send(row T) error
	SendRK(row T, routingKey string) error
	Close() error
}
