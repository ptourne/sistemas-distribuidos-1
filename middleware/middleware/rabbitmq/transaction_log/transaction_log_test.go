package transaction_log

import (
	"fmt"
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

	t.Run("Test Multiple Clients Isolation", func(t *testing.T) {
		testWithOps(t, []Op{
			// Client 1 transactions
			{received: &received{1, 1, ReceivedType_Normal, []byte("client1_data1")}},
			{acknowledged: &acknowledged{1, 1}},
			{received: &received{1, 2, ReceivedType_Normal, []byte("client1_data2")}},
			{acknowledged: &acknowledged{1, 2}},

			// Client 2 transactions
			{received: &received{2, 1, ReceivedType_Normal, []byte("client2_data1")}},
			{acknowledged: &acknowledged{2, 1}},
			{received: &received{2, 3, ReceivedType_Normal, []byte("client2_data3")}},
			{acknowledged: &acknowledged{2, 3}},

			// Client 3 transactions
			{received: &received{3, 5, ReceivedType_Normal, []byte("client3_data5")}},
			{acknowledged: &acknowledged{3, 5}},
		}, func(tl TransactionLog) {
			// Verify each client's transactions are tracked correctly
			assert.True(t, tl.IsDuplicate(1, 1), "Client 1 transaction 1 should be duplicate")
			assert.True(t, tl.IsDuplicate(1, 2), "Client 1 transaction 2 should be duplicate")
			assert.True(t, tl.IsDuplicate(2, 1), "Client 2 transaction 1 should be duplicate")
			assert.True(t, tl.IsDuplicate(2, 3), "Client 2 transaction 3 should be duplicate")
			assert.True(t, tl.IsDuplicate(3, 5), "Client 3 transaction 5 should be duplicate")

			// Verify cross-client isolation - transactions from one client should not affect another
			assert.False(t, tl.IsDuplicate(1, 3), "Client 1 should not have transaction 3")
			assert.False(t, tl.IsDuplicate(2, 5), "Client 2 should not have transaction 5")

			// Verify non-existent clients don't interfere
			assert.False(t, tl.IsDuplicate(99, 1), "Non-existent client 99 should not have any transactions")
		})
	})

	t.Run("Test EOF Messages Don't Mix Between Clients", func(t *testing.T) {
		testWithOps(t, []Op{
			// Normal and EOF messages for different clients
			{received: &received{1, 1, ReceivedType_Normal, []byte("client1_normal")}},
			{acknowledged: &acknowledged{1, 1}},
			{received: &received{2, 1, ReceivedType_EOF, nil}},
			{acknowledged: &acknowledged{2, 1}},
			{received: &received{1, 2, ReceivedType_EOF, nil}},
			{acknowledged: &acknowledged{1, 2}},
			{received: &received{3, 10, ReceivedType_Normal, []byte("client3_normal")}},
			{acknowledged: &acknowledged{3, 10}},
		}, func(tl TransactionLog) {
			// Verify each client's transactions are properly isolated
			assert.True(t, tl.IsDuplicate(1, 1), "Client 1 normal message should be duplicate")
			assert.True(t, tl.IsDuplicate(1, 2), "Client 1 EOF message should be duplicate")
			assert.True(t, tl.IsDuplicate(2, 1), "Client 2 EOF message should be duplicate")
			assert.True(t, tl.IsDuplicate(3, 10), "Client 3 normal message should be duplicate")

			// Verify cross-client isolation
			assert.False(t, tl.IsDuplicate(1, 10), "Client 1 should not have transaction 10")
			assert.False(t, tl.IsDuplicate(2, 2), "Client 2 should not have transaction 2")
		})
	})

	t.Run("Test Large Number of Clients", func(t *testing.T) {
		var ops []Op
		numClients := 100
		transactionsPerClient := 10

		// Generate transactions for many clients
		for cid := uint64(1); cid <= uint64(numClients); cid++ {
			for id := uint64(1); id <= uint64(transactionsPerClient); id++ {
				data := []byte(fmt.Sprintf("client%d_msg%d", cid, id))
				ops = append(ops, Op{received: &received{cid, id, ReceivedType_Normal, data}})
				ops = append(ops, Op{acknowledged: &acknowledged{cid, id}})
			}
		}

		testWithOps(t, ops, func(tl TransactionLog) {
			// Verify all transactions are properly tracked per client
			for cid := uint64(1); cid <= uint64(numClients); cid++ {
				for id := uint64(1); id <= uint64(transactionsPerClient); id++ {
					assert.True(t, tl.IsDuplicate(cid, id),
						"Client %d transaction %d should be duplicate", cid, id)
				}
				// Verify higher IDs are not duplicates
				assert.False(t, tl.IsDuplicate(cid, uint64(transactionsPerClient+1)),
					"Client %d transaction %d should not be duplicate", cid, transactionsPerClient+1)
			}

			// Verify non-existent client doesn't have transactions
			assert.False(t, tl.IsDuplicate(uint64(numClients+1), 1),
				"Non-existent client should not have any transactions")
		})
	})

	t.Run("Test Client ID Edge Cases", func(t *testing.T) {
		testWithOps(t, []Op{
			// Test with client ID 0
			{received: &received{0, 1, ReceivedType_Normal, []byte("client0_data")}},
			{acknowledged: &acknowledged{0, 1}},

			// Test with very large client ID
			{received: &received{^uint64(0), 1, ReceivedType_Normal, []byte("max_client_data")}},
			{acknowledged: &acknowledged{^uint64(0), 1}},

			// Test with regular client ID
			{received: &received{42, 1, ReceivedType_Normal, []byte("client42_data")}},
			{acknowledged: &acknowledged{42, 1}},
		}, func(tl TransactionLog) {
			// Verify each client ID is handled correctly
			assert.True(t, tl.IsDuplicate(0, 1), "Client 0 transaction should be duplicate")
			assert.True(t, tl.IsDuplicate(^uint64(0), 1), "Max client ID transaction should be duplicate")
			assert.True(t, tl.IsDuplicate(42, 1), "Client 42 transaction should be duplicate")

			// Verify isolation between edge case clients
			assert.False(t, tl.IsDuplicate(0, 2), "Client 0 should not have transaction 2")
			assert.False(t, tl.IsDuplicate(^uint64(0), 2), "Max client should not have transaction 2")
			assert.False(t, tl.IsDuplicate(42, 2), "Client 42 should not have transaction 2")

			// Verify cross-client isolation with edge cases
			assert.False(t, tl.IsDuplicate(1, 1), "Client 1 should not have any transactions")
			assert.False(t, tl.IsDuplicate(41, 1), "Client 41 should not have any transactions")
			assert.False(t, tl.IsDuplicate(43, 1), "Client 43 should not have any transactions")
		})
	})

	t.Run("Test Recovery Preserves Client Isolation", func(t *testing.T) {
		// This test specifically verifies that after recovery from disk,
		// client isolation is maintained
		testWithOps(t, []Op{
			{received: &received{100, 5, ReceivedType_Normal, []byte("client100_before_recovery")}},
			{acknowledged: &acknowledged{100, 5}},
			{received: &received{200, 3, ReceivedType_Normal, []byte("client200_before_recovery")}},
			{acknowledged: &acknowledged{200, 3}},
			{received: &received{100, 10, ReceivedType_Normal, []byte("client100_after_recovery")}},
			{acknowledged: &acknowledged{100, 10}},
			{received: &received{300, 1, ReceivedType_Normal, []byte("client300_new")}},
			{acknowledged: &acknowledged{300, 1}},
		}, func(tl TransactionLog) {
			// Verify all client transactions are preserved after recovery
			assert.True(t, tl.IsDuplicate(100, 5), "Client 100 transaction 5 should be duplicate after recovery")
			assert.True(t, tl.IsDuplicate(100, 10), "Client 100 transaction 10 should be duplicate after recovery")
			assert.True(t, tl.IsDuplicate(200, 3), "Client 200 transaction 3 should be duplicate after recovery")
			assert.True(t, tl.IsDuplicate(300, 1), "Client 300 transaction 1 should be duplicate after recovery")

			// Verify client isolation is maintained after recovery
			assert.False(t, tl.IsDuplicate(200, 5), "Client 200 should not have client 100's transaction")
			assert.False(t, tl.IsDuplicate(300, 5), "Client 300 should not have client 100's transaction")
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
