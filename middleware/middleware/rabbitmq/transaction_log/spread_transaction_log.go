package transaction_log

import (
	"container/list"
	"fmt"
	"io"

	"github.com/ptourne/sistemas-distribuidos-1/middleware/codec"
)

type disorderedTransactionLog struct {
	writer             io.Writer
	openedTransaction  *Transaction
	closedTransactions map[uint64]bool
}

func newDisorderedTransactionLog(writer io.Writer) *disorderedTransactionLog {
	return &disorderedTransactionLog{
		writer:             writer,
		openedTransaction:  nil,
		closedTransactions: make(map[uint64]bool, 0),
	}
}

func (t *disorderedTransactionLog) Received(id uint64, data []byte) error {
	received := received{id, data}
	t.received(received)
	buf := received.Encode()
	if err := codec.DoWrite(buf, t.writer); err != nil {
		return fmt.Errorf("failed to write received log: %w", err)
	}
	return nil
}

func (t *disorderedTransactionLog) Acknowledged(id uint64) error {
	t.closeTransaction(id)
	buf := acknowledged{id}.Encode()
	if err := codec.DoWrite(buf, t.writer); err != nil {
		return fmt.Errorf("failed to write acknowledged log: %w", err)
	}
	return nil
}

func (t disorderedTransactionLog) OpenedTransaction() *Transaction {
	return t.openedTransaction
}

func (t *disorderedTransactionLog) IsDuplicate(id uint64) bool {
	return t.closedTransactions[id]
}

type contiguousBlock struct {
	firstId uint64
	lastId  uint64
}

func NewContiguousBlock(id uint64) *contiguousBlock {
	return &contiguousBlock{
		firstId: id,
		lastId:  id,
	}
}

func ToBlock(e *list.Element) *contiguousBlock {
	return e.Value.(*contiguousBlock)
}

func (t *disorderedTransactionLog) received(log received) {
	// t.openedTransactions[log.id] = log.data
	if t.openedTransaction != nil {
		panic("received log while another transaction is open")
	}
	t.openedTransaction = &Transaction{
		Id:   log.id,
		Data: log.data,
	}
}

func (instance *disorderedTransactionLog) closeTransaction(id uint64) {
	fmt.Printf("closing transaction %d\n", id)

	if instance.openedTransaction == nil {
		panic("acknowledged log while no transaction is open")
	}
	if instance.openedTransaction.Id != id {
		panic(fmt.Sprintf("acknowledged log id '%d' does not match opened transaction id '%d'", id, instance.openedTransaction.Id))
	}
	instance.closedTransactions[id] = true
	instance.openedTransaction = nil
}
