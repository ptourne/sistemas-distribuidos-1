package middleware

import (
	"context"
	"fmt"

	"github.com/ptourne/sistemas-distribuidos-1/middleware/codec"
)

// ver de declare, tratr de devovler un struct, ghacer read/write
type Connection[T codec.Serializable[T]] interface {
	ConsumeFrom(sourceName string, groupName string, routingkey string, prefetch int, consumerCount uint) (Receiver[T], error) // CONSUMER COUNT ES LA CANTIDAD DE SHARDS
	WriteTo(writeExchangeName string, subscribers []string, idWorker string, consumerCount uint) (Sender[T], error)            //
	// CONSUMER COUNT ES LA CANTIDAD DE SHARDS
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
	Cid() uint64
	Type() TypeMsg
	Id() uint64
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
	Send(row T, cid uint64, id uint64) error                      //usa el rk del sender id
	SendRK(row T, cid uint64, id uint64, routingKey string) error // manda el row a ese rk
	Prune(cid uint64) error
	SendEOF(cid uint64) error               // manda el EOF a todos los shards
	SendEOFONE(cid uint64, rk string) error //manda el EOF a ese rk NO USAR ES SOLO TESTING
	Close() error
}
