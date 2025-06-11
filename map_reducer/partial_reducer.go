package map_reducer

import (
	"github.com/ptourne/sistemas-distribuidos-1/middleware/codec"
	"github.com/ptourne/sistemas-distribuidos-1/middleware/middleware"
	"github.com/ptourne/sistemas-distribuidos-1/middleware/middleware/rabbitmq/transaction_log"
)

// Received(cid, id uint64, data []byte) error
// ReceivedEOF(cid uint64) error
// Acknowledged() error
// FromCheckpoint(data []byte) error
// Dump() []byte

type PartialReducer[I codec.Serializable[I], A codec.Serializable[A], R codec.Serializable[R]] struct {
	Input             middleware.Receiver[I]
	PartReduceBatches map[string]*A
	FinalReduceSender middleware.Sender[A]
	connIn            middleware.Connection[I]
	transactionLog    transaction_log.TransactionLog
}
