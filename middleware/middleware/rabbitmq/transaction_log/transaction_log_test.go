package transaction_log

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

type Op struct {
	*received
	*acknowledged
}

func TestTransactionLog(t *testing.T) {

	t.Run("Test Opened Transaction", func(t *testing.T) {
		testWithOps(t, []Op{
			{received: &received{1, []byte("data")}},
		}, func(tl *TransactionLog) {
			assert.Equal(t, map[uint64][]byte{
				1: []byte("data"),
			}, tl.OpenTransactions())
		})
	})

	t.Run("Test Closed Transaction", func(t *testing.T) {
		testWithOps(t, []Op{
			{received: &received{1, []byte("data")}},
			{acknowledged: &acknowledged{1}},
		}, func(tl *TransactionLog) {
			assert.Equal(t, map[uint64][]byte{}, tl.OpenTransactions())
		})
	})

	t.Run("Test Non-Contiguous Transactions", func(t *testing.T) {
		testWithOps(t, []Op{
			{received: &received{1, []byte("data")}},
			{acknowledged: &acknowledged{1}},
			{received: &received{3, []byte("data")}},
			{acknowledged: &acknowledged{3}},
		}, func(tl *TransactionLog) {
			assert.Equal(t, map[uint64][]byte{}, tl.OpenTransactions())
			assert.Equal(t, 1, tl.closedNonContiguousTransactions.Len())
			assert.Equal(t, &contiguousBlock{3, 3}, tl.closedNonContiguousTransactions.Front().Value.(*contiguousBlock))
		})
	})

	t.Run("Test Long Non-Contiguous Transactions", func(t *testing.T) {
		testWithOps(t, []Op{
			{received: &received{1, []byte("data")}},
			{acknowledged: &acknowledged{1}},
			{received: &received{3, []byte("data")}},
			{received: &received{4, []byte("data")}},
			{acknowledged: &acknowledged{3}},
			{acknowledged: &acknowledged{4}},
		}, func(tl *TransactionLog) {
			assert.Equal(t, map[uint64][]byte{}, tl.OpenTransactions())
			assert.Equal(t, 1, tl.closedNonContiguousTransactions.Len())
			assert.Equal(t, &contiguousBlock{3, 4}, tl.closedNonContiguousTransactions.Front().Value.(*contiguousBlock))
		})
	})
}

func testWithOps(t *testing.T, ops []Op, tests func(tl *TransactionLog)) {
	// Create a mock file
	f := newMockFile()

	// Create a new TransactionLog instance
	tl := NewTransactionLog(f)

	for _, op := range ops {
		var err error
		if op.received != nil {
			err = tl.Received(op.received.id, op.received.data)
		} else if op.acknowledged != nil {
			err = tl.Acknowledged(op.acknowledged.id)
		}
		assert.NoError(t, err)
	}

	t.Logf("TransactionLog: %+v", tl)

	tests(tl)

	// Check output
	fmt.Printf("File: %X\n", f.Bytes())
	fmt.Printf("File: %s\n", f.String())

	// r := bytes.NewBuffer(f)
	nw := newMockFile()
	recoveredTL := NewTransactionLogFrom(f, nw)
	assert.NotNil(t, recoveredTL, "Expected TransactionLog to be created from file")
	t.Logf("RecoveredTransactionLog: %+v", recoveredTL)

	AssertEqualTL(t, tl, recoveredTL)

	tests(recoveredTL)
}

func AssertEqualTL(t *testing.T, tl *TransactionLog, recoveredTL *TransactionLog) {
	assert.Equal(t, tl.openedTransactions, recoveredTL.openedTransactions, "Expected opened transactions to match")
	assert.Equal(t, tl.closedNonContiguousTransactions, tl.closedNonContiguousTransactions, "Expected closed non-contiguous transactions to match")
	assert.Equal(t, tl.lastContiguousClosedTransaction, tl.lastContiguousClosedTransaction, "Expected last contiguous closed transaction to match")
}

func newMockFile() *bytes.Buffer {
	return &bytes.Buffer{}
	// return mockFile, writer

	// file, err := os.OpenFile("test.log", os.O_RDWR|os.O_CREATE, 0666)
	// if err != nil {
	// 	panic(err)
	// }
	// mockFile = make([]byte, 0, 1000)
	// return mockFile, file
}
