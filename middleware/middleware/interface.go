package middleware

import (
	"context"
	"fmt"

	"github.com/ptourne/sistemas-distribuidos-1/middleware/codec"
)

// ver de declare, tratr de devovler un struct, ghacer read/write
type Connection[T codec.Serializable[T]] interface {
	ConsumeFrom(sourceName string, groupName string, routingkey string, prefetch int) (Receiver[T], error)
	WriteTo(writeExchangeName string, subscribers []string, idWorker string) (Sender[T], error)
	Close() error
}

type Receiver[T codec.Serializable[T]] interface {
	Next(ctx context.Context) (Envelope[T], error)
	Close() error
}

type TimeoutErr struct{}

func (e TimeoutErr) Error() string {
	return "timeout reached while waiting for message"
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
	Normal TypeMsg = iota
	Prune
	EOF
)

// TypeMsg indicates the type received on Next().
//
// - Normal: Normal message received.
// - EOF: Indication that the processing of msgs from this Cid has been completed.
// - Prune: Indication that a peer received an EOF and that this must flush all messages related to that Cid.
func (t TypeMsg) String() string {
	switch t {
	case Normal:
		return "Normal"
	case EOF:
		return "EOF"
	case Prune:
		return "Prune"
	default:
		return "Unknown TypeMsg"
	}
}

func FromStringTypeMsg(s string) (TypeMsg, error) {
	switch s {
	case "Normal":
		return Normal, nil
	case "EOF":
		return EOF, nil
	case "Prune":
		return Prune, nil
	default:
		return -1, fmt.Errorf("unknown TypeMsg: %s", s)
	}
}

type Sender[T codec.Serializable[T]] interface {
	Send(row T, cid string) error      //usa el rk del sender id
	SendMsgID(row T, cid string) error // usa el rk del msj id
	Prune(cid string) error
	SendEOF(cid string) error      //usa el rk del sender id
	SendEOFAllID(cid string) error //manda el EOF a todos los shards
	Close() error
}
