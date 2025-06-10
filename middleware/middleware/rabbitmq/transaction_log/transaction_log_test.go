package transaction_log

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type Op struct {
	*received
	*acknowledged
}

func TestTransactionLog(t *testing.T) {
	t.Run("Test Closed Transaction", func(t *testing.T) {
		testWithOps(t, []Op{
			{received: &received{1, 1, ReceivedType_Normal, []byte("data")}},
			{acknowledged: &acknowledged{1, 1}},
		}, func(tl TransactionLog) {
			assert.True(t, tl.IsDuplicate(1, 1))
			assert.False(t, tl.IsDuplicate(2, 1))
			assert.False(t, tl.IsDuplicate(1, 2))
		})
	})

	t.Run("Test Non-Contiguous Transactions", func(t *testing.T) {
		testWithOps(t, []Op{
			{received: &received{1, 1, ReceivedType_Normal, []byte("data")}},
			{acknowledged: &acknowledged{1, 1}},
			{received: &received{1, 3, ReceivedType_Normal, []byte("data")}},
			{acknowledged: &acknowledged{1, 3}},
		}, func(tl TransactionLog) {
			assert.True(t, tl.IsDuplicate(1, 1))
			assert.True(t, tl.IsDuplicate(1, 3))
		})
	})
}

type mockParent struct {
}

// Received(cid, id uint64, data []byte) error
// ReceivedEOF(cid, id uint64) error
// Acknowledged(cid, id uint64) error
// FromCheckpoint(data []byte) error
// Dump() []byte

func (p *mockParent) Received(cid, id uint64, data []byte) error {
	return nil
}

func (p *mockParent) ReceivedEOF(cid, id uint64) error {
	return nil
}

func (p *mockParent) Acknowledged(cid, id uint64) error {
	return nil
}

func (p *mockParent) FromCheckpoint(data []byte) error {
	return nil
}

func (p *mockParent) Dump() []byte {
	return []byte{}
}

func testWithOps(t *testing.T, ops []Op, tests func(tl TransactionLog)) {
	// Create a mock file
	dirPath := t.TempDir()

	p := &mockParent{}
	// Create a new TransactionLog instance
	tl, err := NewTransactionLogFromDir(dirPath, p)
	assert.NoError(t, err, "Expected no error when creating TransactionLog from directory")

	for _, op := range ops {
		var err error
		if op.received != nil {
			switch op.received.t {
			case ReceivedType_Normal:
				err = tl.Received(op.received.cid, op.received.id, op.received.data)
			case ReceivedType_EOF:
				err = tl.ReceivedEOF(op.received.cid, op.received.id)
			}
		} else if op.acknowledged != nil {
			err = tl.Acknowledged(op.acknowledged.cid, op.acknowledged.id)
		}
		assert.NoError(t, err)
	}

	t.Logf("TransactionLog: %+v", tl)

	tests(tl)

	p2 := &mockParent{}
	recoveredTL, err := NewTransactionLogFromDir(dirPath, p2)
	assert.NoError(t, err, "Expected no error when recovering TransactionLog from directory")
	assert.NotNil(t, recoveredTL, "Expected TransactionLog to be created from file")
	t.Logf("RecoveredTransactionLog: %+v", recoveredTL)

	AssertEqualTL(t, tl, recoveredTL)

	tests(recoveredTL)
}

func AssertEqualTL(t *testing.T, tl TransactionLog, recoveredTL TransactionLog) {
	if otl, ok := tl.(*transactionLog); ok {
		recoveredOtl, ok := recoveredTL.(*transactionLog)
		assert.True(t, ok, "Expected recovered TransactionLog to be of type orderedTransactionLog")
		assert.Equal(t, otl.openedTransaction, recoveredOtl.openedTransaction, "Expected opened transactions to match")
		assert.Equal(t, otl.lastClosedTransactions, recoveredOtl.lastClosedTransactions, "Expected last closed transaction to match")
	} else {
		t.Fatalf("Expected TransactionLog to be of type orderedTransactionLog or disorderedTransactionLog, %+v", tl)
	}
}
