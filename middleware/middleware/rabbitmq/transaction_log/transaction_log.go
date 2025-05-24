package transaction_log

import (
	"fmt"
	"io"

	"container/list"

	"github.com/ptourne/sistemas-distribuidos-1/middleware/codec"
)

type TransactionLog struct {
	writer                          io.Writer
	openedTransactions              map[uint64][]byte
	closedNonContiguousTransactions *list.List
	lastContiguousClosedTransaction uint64
}

func (t TransactionLog) OpenTransactions() map[uint64][]byte {
	return t.openedTransactions
}

func (t TransactionLog) LastContiguousClosedTransaction() uint64 {
	return t.lastContiguousClosedTransaction
}

func (t TransactionLog) ClosedNonContiguousTransactions() *list.List {
	return t.closedNonContiguousTransactions
}

type contiguousBlock struct {
	firstId uint64
	lastId  uint64
}

func (b *contiguousBlock) addIfBelongs(id uint64) (added bool, shouldInsertNew bool) {
	if id < b.firstId-1 {
		return false, false
	}
	if id == b.firstId-1 {
		b.firstId = id
		return true, true
	}
	if id == b.lastId+1 {
		b.lastId = id
		return true, true
	}
	return false, true
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

func (t *TransactionLog) addNonContiguousTransaction(id uint64) {
	if t.closedNonContiguousTransactions.Len() == 0 {
		t.closedNonContiguousTransactions.PushFront(NewContiguousBlock(id))
		return
	}
	element := t.closedNonContiguousTransactions.Front()
	for {
		if element == nil {
			break
		}
		block := ToBlock(element)
		added, shouldInsertNew := block.addIfBelongs(id)
		if added {

			if nextElement := element.Next(); nextElement != nil { // There is a nextBlock block
				if nextBlock := ToBlock(nextElement); block.lastId == nextBlock.firstId-1 { // The next block is contiguous
					// Merge the blocks
					block.lastId = nextBlock.lastId
					t.closedNonContiguousTransactions.Remove(nextElement)
				}
			}
			return
		}
		if shouldInsertNew {
			t.closedNonContiguousTransactions.InsertAfter(NewContiguousBlock(id), element)
		}
		element = element.Next()
	}
}

func NewTransactionLogFrom(reader io.Reader, writer io.Writer) *TransactionLog {
	instance := NewTransactionLog(writer)
	for {
		logType, err := codec.DoRead(1, reader)
		if err != nil {
			break
		}
		fmt.Printf("Received log type: %c\n", logType[0])
		switch LogType(logType[0]) {
		case LogType_Received:
			var log received
			err := log.Decode(reader)
			if err != nil {
				break
			}
			fmt.Printf("Received log: %+v\n", log)
			instance.received(log)
		case LogType_Acknowledged:
			var log acknowledged
			err := log.Decode(reader)
			if err != nil {
				break
			}
			instance.closeTransaction(log.id)
		default:
			fmt.Printf("Unknown log type: %v\n", logType[0])
			return nil
		}
	}
	return instance
}

func (instance *TransactionLog) closeTransaction(id uint64) {
	fmt.Printf("Closing transaction: %d\n", id)
	if _, ok := instance.openedTransactions[id]; ok {
		delete(instance.openedTransactions, id)
		if id == instance.lastContiguousClosedTransaction+1 {
			instance.lastContiguousClosedTransaction = id
			if firstElement := instance.closedNonContiguousTransactions.Front(); firstElement != nil {
				if firstBlock := ToBlock(firstElement); firstBlock.firstId == id+1 {
					instance.lastContiguousClosedTransaction = firstBlock.lastId
					instance.closedNonContiguousTransactions.Remove(firstElement)
				}
			}
		} else {
			instance.addNonContiguousTransaction(id)
		}
	} else {
		panic("acknowledged log id not found in opened transactions")
	}
}

func NewTransactionLog(writer io.Writer) *TransactionLog {
	return &TransactionLog{
		writer:                          writer,
		openedTransactions:              make(map[uint64][]byte),
		closedNonContiguousTransactions: list.New(),
		lastContiguousClosedTransaction: 0,
	}
}

func (t *TransactionLog) received(log received) {
	fmt.Printf("Received log: %+v\n", log)
	t.openedTransactions[log.id] = log.data
}

func (t *TransactionLog) Received(id uint64, data []byte) error {
	received := received{id, data}
	t.received(received)
	buf := received.Encode()
	fmt.Printf("Writting log of len %d: %X\n", len(buf), buf)
	if err := codec.DoWrite(buf, t.writer); err != nil {
		return fmt.Errorf("failed to write received log: %w", err)
	}
	return nil
}

func (t *TransactionLog) Acknowledged(id uint64) error {
	t.closeTransaction(id)
	buf := acknowledged{id}.Encode()
	fmt.Printf("Writting log of len %d: %X\n", len(buf), buf)
	if err := codec.DoWrite(buf, t.writer); err != nil {
		return fmt.Errorf("failed to write acknowledged log: %w", err)
	}
	return nil
}
