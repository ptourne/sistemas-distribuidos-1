package middleware

import (
	"context"
	"fmt"

	"github.com/ptourne/sistemas-distribuidos-1/middleware/codec"
)

// ver de declare, tratr de devovler un struct, ghacer read/write
type Connection[T codec.Serializable[T]] interface {
	ConsumeFrom(sourceName string, groupName string, peers int, prefetch int) (Receiver[T], error)
	ConsumeFromRK(sourceName string, groupName string, t string, routingkey string, peers int, prefetch int) (Receiver[T], error)
	WriteTo(writeExchangeName string, subscribers []string) (Sender[T], error)
	WriteToRK(writeExchangeName string, subscribers map[string][]string, t string) (Sender[T], error)
	Close() error
}

type Receiver[T codec.Serializable[T]] interface {
	Next(ctx context.Context) (Envelope[T], bool, error)
	Close() error
}

type Envelope[T codec.Serializable[T]] interface {
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

func (t TypeMsg) String() string {
	switch t {
	case QueryName:
		return "QueryName"
	case QueryRow:
		return "QueryRow"
	case FinishQuerys:
		return "FinishQuerys"
	case FinishCid:
		return "FinishCid"
	case FinishDone:
		return "FinishDone"
	default:
		return "Unknown TypeMsg"
	}
}

func FromStringTypeMsg(s string) (TypeMsg, error) {
	switch s {
	case "QueryName":
		return QueryName, nil
	case "QueryRow":
		return QueryRow, nil
	case "FinishQuerys":
		return FinishQuerys, nil
	case "FinishCid":
		return FinishCid, nil
	case "FinishDone":
		return FinishDone, nil
	default:
		return -1, fmt.Errorf("unknown TypeMsg: %s", s)
	}
}

type Sender[T codec.Serializable[T]] interface {
	Send(row T, cid string, t TypeMsg) error
	SendRK(row T, routingKey string, cid string, t TypeMsg) error
	Close() error
}
