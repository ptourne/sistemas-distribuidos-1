package transaction_log

import (
	"fmt"
	"io"

	"github.com/ptourne/sistemas-distribuidos-1/middleware/codec"
)

type orderedTransactionLog struct {
	writer                io.Writer
	openedTransaction     *Transaction
	lastClosedTransaction uint64
}

func newOrderedTransactionLog(writer io.Writer) transactionLogInternal {
	return &orderedTransactionLog{
		writer:                writer,
		openedTransaction:     nil,
		lastClosedTransaction: 0,
	}
}

func (t *orderedTransactionLog) Received(id uint64, data []byte) error {
	received := received{id, data}
	t.received(received)
	buf := received.Encode()
	if err := codec.DoWrite(buf, t.writer); err != nil {
		return fmt.Errorf("failed to write received log: %w", err)
	}
	return nil
}

func (t *orderedTransactionLog) Acknowledged(id uint64) error {
	t.closeTransaction(id)
	buf := acknowledged{id}.Encode()
	if err := codec.DoWrite(buf, t.writer); err != nil {
		return fmt.Errorf("failed to write acknowledged log: %w", err)
	}
	return nil
}

func (t *orderedTransactionLog) OpenedTransaction() *Transaction {
	return t.openedTransaction
}

func (t *orderedTransactionLog) IsDuplicate(id uint64) bool {
	return t.lastClosedTransaction >= id
}

func (t *orderedTransactionLog) received(log received) {
	t.openedTransaction = &Transaction{
		Id:   log.id,
		Data: log.data,
	}
}

func (t *orderedTransactionLog) closeTransaction(id uint64) {
	if t.openedTransaction != nil && t.openedTransaction.Id == id {
		t.lastClosedTransaction = id
		t.openedTransaction = nil
	} else {
		panic(fmt.Sprintf("transaction %d not found", id))
	}
}
