package transaction_log

import (
	"io"

	"github.com/ptourne/sistemas-distribuidos-1/middleware/codec"
)

type Transaction struct {
	Id   uint64
	Data []byte
}

type TransactionLog interface {
	Received(id uint64, data []byte) error
	Acknowledged(id uint64) error
	OpenedTransaction() *Transaction
	IsDuplicate(id uint64) bool
}

type transactionLogInternal interface {
	TransactionLog
	received(log received)
	closeTransaction(id uint64)
}

func NewTransactionLogFrom(reader io.Reader, writer io.Writer, spread bool) TransactionLog {
	instance := newTransactionLogInternal(writer, spread)
	for {
		logType, err := codec.DoRead(1, reader)
		if err != nil {
			break
		}
		switch LogType(logType[0]) {
		case LogType_Received:
			var log received
			err := log.Decode(reader)
			if err != nil {
				break
			}
			instance.received(log)
		case LogType_Acknowledged:
			var log acknowledged
			err := log.Decode(reader)
			if err != nil {
				break
			}
			instance.closeTransaction(log.id)
		default:
			return nil
		}
	}
	return instance
}

func NewTransactionLog(writer io.Writer, ordered bool) TransactionLog {
	return newTransactionLogInternal(writer, ordered)
}

func newTransactionLogInternal(writer io.Writer, ordered bool) transactionLogInternal {
	if ordered {
		return newOrderedTransactionLog(writer)
	}
	return newDisorderedTransactionLog(writer)
}
