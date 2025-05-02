package middleware

import (
	"time"

	"github.com/ptourne/sistemas-distribuidos-1/middleware/codec"
)

// ver de declare, tratr de devovler un struct, ghacer read/write
type Connection[T codec.Serializable] interface {
	ConsumeFrom(sourceName string, groupName string, peers int, prefetch int) (Receiver[T], error)
	ConsumeFromRK(sourceName string, groupName string, t string, routingkey string, peers int, prefetch int) (Receiver[T], error)
	WriteTo(writeExchangeName string, subscribers []string) (Sender[T], error)
	WriteToRK(writeExchangeName string, subscribers map[string][]string, t string) (Sender[T], error)
	Close() error
}

type Receiver[T codec.Serializable] interface {
	Next(timeout *time.Timer) (Envelope[T], bool, error)
	Qos(int, int) error
	Close() error
}

type Envelope[T codec.Serializable] interface {
	Msg() T
	Ack(multiple bool) error
	Nack(multiple bool) error
	Cid() string
	Type() TypeMsg
}


type TypeMsg int

const (
	QueryName TypeMsg = iota
	QueryRow
	FinishQuerys
	FinishCid
	FinishDone
)

type Sender[T codec.Serializable] interface {
	Send(row *T, cid string, t TypeMsg) error
	SendRK(row *T, routingKey string, cid string, t TypeMsg) error
	Close() error
}
