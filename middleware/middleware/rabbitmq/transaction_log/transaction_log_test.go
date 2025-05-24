package transaction_log

import (
	"bytes"
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
		}, func(tl TransactionLog) {
			assert.Equal(t, &Transaction{Id: 1, Data: []byte("data")}, tl.OpenedTransaction())
		})
	})

	t.Run("Test Closed Transaction", func(t *testing.T) {
		testWithOps(t, []Op{
			{received: &received{1, []byte("data")}},
			{acknowledged: &acknowledged{1}},
		}, func(tl TransactionLog) {
			assert.Nil(t, tl.OpenedTransaction())
			assert.True(t, tl.IsDuplicate(1))
		})
	})

	t.Run("Test Non-Contiguous Transactions", func(t *testing.T) {
		testWithOps(t, []Op{
			{received: &received{1, []byte("data")}},
			{acknowledged: &acknowledged{1}},
			{received: &received{3, []byte("data")}},
			{acknowledged: &acknowledged{3}},
		}, func(tl TransactionLog) {
			assert.True(t, tl.IsDuplicate(1))
		})
	})

	t.Run("Test Long Non-Contiguous Transactions", func(t *testing.T) {
		testWithOps(t, []Op{
			{received: &received{1, []byte("data")}},
			{acknowledged: &acknowledged{1}},
			{received: &received{3, []byte("data")}},
			{acknowledged: &acknowledged{3}},
			{received: &received{4, []byte("data")}},
		}, func(tl TransactionLog) {
			assert.True(t, tl.IsDuplicate(1))
			assert.False(t, tl.IsDuplicate(2))
			assert.True(t, tl.IsDuplicate(3))
			assert.False(t, tl.IsDuplicate(4))
		})
	})
}

func testWithOps(t *testing.T, ops []Op, tests func(tl TransactionLog)) {
	// Create a mock file
	f := newMockFile()

	// Create a new TransactionLog instance
	tl := NewTransactionLog(f, false)

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
	t.Logf("File: %X\n", f.Bytes())
	t.Logf("File: %s\n", f.String())

	// r := bytes.NewBuffer(f)
	nw := newMockFile()
	recoveredTL := NewTransactionLogFrom(f, nw, false)
	assert.NotNil(t, recoveredTL, "Expected TransactionLog to be created from file")
	t.Logf("RecoveredTransactionLog: %+v", recoveredTL)

	AssertEqualTL(t, tl, recoveredTL)

	tests(recoveredTL)
}

func AssertEqualTL(t *testing.T, tl TransactionLog, recoveredTL TransactionLog) {
	if otl, ok := tl.(*orderedTransactionLog); ok {
		recoveredOtl, ok := recoveredTL.(*orderedTransactionLog)
		assert.True(t, ok, "Expected recovered TransactionLog to be of type orderedTransactionLog")
		assert.Equal(t, otl.openedTransaction, recoveredOtl.openedTransaction, "Expected opened transactions to match")
		assert.Equal(t, otl.lastClosedTransaction, recoveredOtl.lastClosedTransaction, "Expected last closed transaction to match")
	} else if dtl, ok := tl.(*disorderedTransactionLog); ok {
		recoveredDtl, ok := recoveredTL.(*disorderedTransactionLog)
		assert.True(t, ok, "Expected TransactionLog to be of type disorderedTransactionLog")
		assert.Equal(t, dtl.openedTransaction, recoveredDtl.openedTransaction, "Expected opened transactions to match")
		assert.Equal(t, dtl.closedTransactions, recoveredDtl.closedTransactions, "Expected last contiguous closed transaction to match")
	} else {
		t.Fatalf("Expected TransactionLog to be of type orderedTransactionLog or disorderedTransactionLog, %+v", tl)
	}
}

func newMockFile() *bytes.Buffer {
	return &bytes.Buffer{}
}
