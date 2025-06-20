package transaction_log

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

type Op struct {
	*receivedNormal
	*receivedEof
	*acknowledged
	*dump
}

type dump struct {
}

func TestTransactionLog(t *testing.T) {
	t.Run("Test Closed Transaction", func(t *testing.T) {
		testWithOps(t, []Op{
			{receivedNormal: &receivedNormal{1, 1, []byte("data")}},
			{acknowledged: &acknowledged{}},
		}, func(tl TransactionLog) {
			assert.True(t, tl.IsDuplicate(1, 1))
			assert.False(t, tl.IsDuplicate(2, 1))
			assert.False(t, tl.IsDuplicate(1, 2))
		})
	})

	t.Run("Test Non-Contiguous Transactions", func(t *testing.T) {
		testWithOps(t, []Op{
			{receivedNormal: &receivedNormal{1, 1, []byte("data")}},
			{acknowledged: &acknowledged{}},
			{receivedNormal: &receivedNormal{1, 3, []byte("data")}},
			{acknowledged: &acknowledged{}},
		}, func(tl TransactionLog) {
			assert.True(t, tl.IsDuplicate(1, 1))
			assert.True(t, tl.IsDuplicate(1, 3))
		})
	})

	t.Run("Test Multiple Clients Isolation", func(t *testing.T) {
		testWithOps(t, []Op{
			// Client 1 transactions
			{receivedNormal: &receivedNormal{1, 1, []byte("client1_data1")}},
			{acknowledged: &acknowledged{}},
			{receivedNormal: &receivedNormal{1, 2, []byte("client1_data2")}},
			{acknowledged: &acknowledged{}},

			// Client 2 transactions
			{receivedNormal: &receivedNormal{2, 1, []byte("client2_data1")}},
			{acknowledged: &acknowledged{}},
			{receivedNormal: &receivedNormal{2, 3, []byte("client2_data3")}},
			{acknowledged: &acknowledged{}},

			// Client 3 transactions
			{receivedNormal: &receivedNormal{3, 5, []byte("client3_data5")}},
			{acknowledged: &acknowledged{}},
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
			{receivedNormal: &receivedNormal{1, 1, []byte("client1_normal")}},
			{acknowledged: &acknowledged{}},
			{receivedEof: &receivedEof{2}},
			{acknowledged: &acknowledged{}},
			{receivedEof: &receivedEof{1}},
			{acknowledged: &acknowledged{}},
			{receivedNormal: &receivedNormal{3, 10, []byte("client3_normal")}},
			{acknowledged: &acknowledged{}},
		}, func(tl TransactionLog) {
			// Verify each client's transactions are properly isolated
			assert.True(t, tl.IsDuplicate(3, 10), "Client 3 normal message should be duplicate")

			// Verify cross-client isolation
			assert.False(t, tl.HasTransactions(1), "Client 1 EOF message should not be duplicate")
			assert.False(t, tl.HasTransactions(2), "Client 2 EOF message should not be duplicate")
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
				ops = append(ops, Op{receivedNormal: &receivedNormal{cid, id, data}})
				ops = append(ops, Op{acknowledged: &acknowledged{}})
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
			{receivedNormal: &receivedNormal{0, 1, []byte("client0_data")}},
			{acknowledged: &acknowledged{}},

			// Test with very large client ID
			{receivedNormal: &receivedNormal{^uint64(0), 1, []byte("max_client_data")}},
			{acknowledged: &acknowledged{}},

			// Test with regular client ID
			{receivedNormal: &receivedNormal{42, 1, []byte("client42_data")}},
			{acknowledged: &acknowledged{}},
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
			{receivedNormal: &receivedNormal{100, 5, []byte("client100_before_recovery")}},
			{acknowledged: &acknowledged{}},
			{receivedNormal: &receivedNormal{200, 3, []byte("client200_before_recovery")}},
			{acknowledged: &acknowledged{}},
			{receivedNormal: &receivedNormal{100, 10, []byte("client100_after_recovery")}},
			{acknowledged: &acknowledged{}},
			{receivedNormal: &receivedNormal{300, 1, []byte("client300_new")}},
			{acknowledged: &acknowledged{}},
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

	t.Run("Test Checkpoint Basic Functionality", func(t *testing.T) {
		testWithOps(t, []Op{
			// Some initial transactions
			{receivedNormal: &receivedNormal{1, 1, []byte("before_checkpoint_1")}},
			{acknowledged: &acknowledged{}},
			{receivedNormal: &receivedNormal{2, 1, []byte("before_checkpoint_2")}},
			{acknowledged: &acknowledged{}},

			// Create checkpoint
			{dump: &dump{}},

			// Transactions after checkpoint
			{receivedNormal: &receivedNormal{1, 2, []byte("after_checkpoint_1")}},
			{acknowledged: &acknowledged{}},
			{receivedNormal: &receivedNormal{3, 1, []byte("after_checkpoint_3")}},
			{acknowledged: &acknowledged{}},
		}, func(tl TransactionLog) {
			// Verify all transactions are tracked correctly after checkpoint
			assert.True(t, tl.IsDuplicate(1, 1), "Client 1 transaction 1 should be duplicate")
			assert.True(t, tl.IsDuplicate(1, 2), "Client 1 transaction 2 should be duplicate")
			assert.True(t, tl.IsDuplicate(2, 1), "Client 2 transaction 1 should be duplicate")
			assert.True(t, tl.IsDuplicate(3, 1), "Client 3 transaction 1 should be duplicate")

			// Verify non-existent transactions
			assert.False(t, tl.IsDuplicate(1, 3), "Client 1 should not have transaction 3")
			assert.False(t, tl.IsDuplicate(2, 2), "Client 2 should not have transaction 2")
		})
	})

	t.Run("Test Multiple Checkpoints", func(t *testing.T) {
		testWithOps(t, []Op{
			// Initial transactions
			{receivedNormal: &receivedNormal{1, 1, []byte("data1")}},
			{acknowledged: &acknowledged{}},
			{receivedNormal: &receivedNormal{2, 1, []byte("data2")}},
			{acknowledged: &acknowledged{}},

			// First checkpoint
			{dump: &dump{}},

			// More transactions
			{receivedNormal: &receivedNormal{1, 2, []byte("data3")}},
			{acknowledged: &acknowledged{}},
			{receivedNormal: &receivedNormal{3, 1, []byte("data4")}},
			{acknowledged: &acknowledged{}},

			// Second checkpoint
			{dump: &dump{}},

			// Final transactions
			{receivedNormal: &receivedNormal{1, 3, []byte("data5")}},
			{acknowledged: &acknowledged{}},
		}, func(tl TransactionLog) {
			// All transactions should be preserved across multiple checkpoints
			assert.True(t, tl.IsDuplicate(1, 1), "Client 1 transaction 1 should be duplicate")
			assert.True(t, tl.IsDuplicate(1, 2), "Client 1 transaction 2 should be duplicate")
			assert.True(t, tl.IsDuplicate(1, 3), "Client 1 transaction 3 should be duplicate")
			assert.True(t, tl.IsDuplicate(2, 1), "Client 2 transaction 1 should be duplicate")
			assert.True(t, tl.IsDuplicate(3, 1), "Client 3 transaction 1 should be duplicate")
		})
	})

	t.Run("Test Checkpoint With EOF Messages", func(t *testing.T) {
		testWithOps(t, []Op{
			// Normal transactions
			{receivedNormal: &receivedNormal{1, 1, []byte("normal1")}},
			{acknowledged: &acknowledged{}},
			{receivedNormal: &receivedNormal{2, 1, []byte("normal2")}},
			{acknowledged: &acknowledged{}},

			// EOF for one client
			{receivedEof: &receivedEof{1}},
			{acknowledged: &acknowledged{}},

			// Create checkpoint
			{dump: &dump{}},

			// More transactions after checkpoint
			{receivedNormal: &receivedNormal{2, 2, []byte("normal3")}},
			{acknowledged: &acknowledged{}},
			{receivedEof: &receivedEof{2}},
			{acknowledged: &acknowledged{}},
		}, func(tl TransactionLog) {
			// Client 1 should not have transactions after EOF
			assert.False(t, tl.HasTransactions(1), "Client 1 should not have transactions after EOF")

			// Client 2 should not have transactions after EOF
			assert.False(t, tl.HasTransactions(2), "Client 2 should not have transactions after EOF")

			// Verify cross-client isolation
			assert.False(t, tl.IsDuplicate(1, 2), "Client 1 should not have transaction 2")
			assert.False(t, tl.IsDuplicate(2, 3), "Client 2 should not have transaction 3")
		})
	})

	t.Run("Test Checkpoint Recovery Preserves Client Isolation", func(t *testing.T) {
		testWithOps(t, []Op{
			// Transactions for multiple clients
			{receivedNormal: &receivedNormal{10, 5, []byte("client10_pre_checkpoint")}},
			{acknowledged: &acknowledged{}},
			{receivedNormal: &receivedNormal{20, 3, []byte("client20_pre_checkpoint")}},
			{acknowledged: &acknowledged{}},
			{receivedNormal: &receivedNormal{30, 7, []byte("client30_pre_checkpoint")}},
			{acknowledged: &acknowledged{}},

			// Create checkpoint
			{dump: &dump{}},

			// More transactions after checkpoint
			{receivedNormal: &receivedNormal{10, 6, []byte("client10_post_checkpoint")}},
			{acknowledged: &acknowledged{}},
			{receivedNormal: &receivedNormal{40, 1, []byte("client40_new")}},
			{acknowledged: &acknowledged{}},
		}, func(tl TransactionLog) {
			// Verify all client transactions are preserved
			assert.True(t, tl.IsDuplicate(10, 5), "Client 10 transaction 5 should be duplicate")
			assert.True(t, tl.IsDuplicate(10, 6), "Client 10 transaction 6 should be duplicate")
			assert.True(t, tl.IsDuplicate(20, 3), "Client 20 transaction 3 should be duplicate")
			assert.True(t, tl.IsDuplicate(30, 7), "Client 30 transaction 7 should be duplicate")
			assert.True(t, tl.IsDuplicate(40, 1), "Client 40 transaction 1 should be duplicate")

			// Verify client isolation is maintained
			assert.False(t, tl.IsDuplicate(10, 7), "Client 10 should not have transaction 7")
			assert.False(t, tl.IsDuplicate(20, 5), "Client 20 should not have transaction 5")
			assert.False(t, tl.IsDuplicate(30, 8), "Client 30 should not have transaction 8")
			assert.False(t, tl.IsDuplicate(40, 5), "Client 40 should not have transaction 5")
		})
	})

	t.Run("Test Checkpoint With Interleaved Operations", func(t *testing.T) {
		testWithOps(t, []Op{
			// Interleaved operations between multiple clients
			{receivedNormal: &receivedNormal{100, 1, []byte("client100_msg1")}},
			{receivedNormal: &receivedNormal{200, 1, []byte("client200_msg1")}},
			{acknowledged: &acknowledged{}}, // acks client 100, msg 1
			{receivedNormal: &receivedNormal{100, 2, []byte("client100_msg2")}},
			{acknowledged: &acknowledged{}}, // acks client 200, msg 1
			{receivedNormal: &receivedNormal{300, 1, []byte("client300_msg1")}},
			{acknowledged: &acknowledged{}}, // acks client 100, msg 2

			// Create checkpoint
			{dump: &dump{}},

			// More interleaved operations
			{receivedNormal: &receivedNormal{200, 2, []byte("client200_msg2")}},
			{acknowledged: &acknowledged{}}, // acks client 300, msg 1
			{receivedNormal: &receivedNormal{100, 3, []byte("client100_msg3")}},
			{acknowledged: &acknowledged{}}, // acks client 200, msg 2
			{acknowledged: &acknowledged{}}, // acks client 100, msg 3
		}, func(tl TransactionLog) {
			// Verify all transactions are correctly tracked per client
			assert.True(t, tl.IsDuplicate(100, 1), "Client 100 transaction 1 should be duplicate")
			assert.True(t, tl.IsDuplicate(100, 2), "Client 100 transaction 2 should be duplicate")
			assert.True(t, tl.IsDuplicate(100, 3), "Client 100 transaction 3 should be duplicate")
			assert.True(t, tl.IsDuplicate(200, 1), "Client 200 transaction 1 should be duplicate")
			assert.True(t, tl.IsDuplicate(200, 2), "Client 200 transaction 2 should be duplicate")
			assert.True(t, tl.IsDuplicate(300, 1), "Client 300 transaction 1 should be duplicate")

			// Verify cross-client isolation
			assert.False(t, tl.IsDuplicate(100, 4), "Client 100 should not have transaction 4")
			assert.False(t, tl.IsDuplicate(200, 3), "Client 200 should not have transaction 3")
			assert.False(t, tl.IsDuplicate(300, 2), "Client 300 should not have transaction 2")
		})
	})

	t.Run("Test Checkpoint Recovery With Large Client IDs", func(t *testing.T) {
		testWithOps(t, []Op{
			// Test with edge case client IDs
			{receivedNormal: &receivedNormal{0, 1, []byte("client0_data")}},
			{acknowledged: &acknowledged{}},
			{receivedNormal: &receivedNormal{^uint64(0), 1, []byte("max_client_data")}},
			{acknowledged: &acknowledged{}},
			{receivedNormal: &receivedNormal{^uint64(0) - 1, 5, []byte("near_max_client_data")}},
			{acknowledged: &acknowledged{}},

			// Create checkpoint
			{dump: &dump{}},

			// More transactions with edge case IDs
			{receivedNormal: &receivedNormal{0, 2, []byte("client0_data2")}},
			{acknowledged: &acknowledged{}},
			{receivedNormal: &receivedNormal{^uint64(0), 2, []byte("max_client_data2")}},
			{acknowledged: &acknowledged{}},
		}, func(tl TransactionLog) {
			// Verify edge case client IDs work correctly with checkpoints
			assert.True(t, tl.IsDuplicate(0, 1), "Client 0 transaction 1 should be duplicate")
			assert.True(t, tl.IsDuplicate(0, 2), "Client 0 transaction 2 should be duplicate")
			assert.True(t, tl.IsDuplicate(^uint64(0), 1), "Max client transaction 1 should be duplicate")
			assert.True(t, tl.IsDuplicate(^uint64(0), 2), "Max client transaction 2 should be duplicate")
			assert.True(t, tl.IsDuplicate(^uint64(0)-1, 5), "Near-max client transaction 5 should be duplicate")

			// Verify isolation between edge case clients
			assert.False(t, tl.IsDuplicate(0, 5), "Client 0 should not have transaction 5")
			assert.False(t, tl.IsDuplicate(^uint64(0), 5), "Max client should not have transaction 5")
		})
	})

	t.Run("Test Cross-Client Isolation With Checkpoints", func(t *testing.T) {
		testWithOps(t, []Op{
			// Multiple clients with same transaction IDs but different cids
			{receivedNormal: &receivedNormal{1, 5, []byte("client1_transaction5")}},
			{acknowledged: &acknowledged{}},
			{receivedNormal: &receivedNormal{2, 5, []byte("client2_transaction5")}},
			{acknowledged: &acknowledged{}},
			{receivedNormal: &receivedNormal{3, 5, []byte("client3_transaction5")}},
			{acknowledged: &acknowledged{}},

			// Create checkpoint
			{dump: &dump{}},

			// More transactions with same IDs
			{receivedNormal: &receivedNormal{1, 10, []byte("client1_transaction10")}},
			{acknowledged: &acknowledged{}},
			{receivedNormal: &receivedNormal{2, 10, []byte("client2_transaction10")}},
			{acknowledged: &acknowledged{}},
		}, func(tl TransactionLog) {
			// Each client should only know about its own transactions
			assert.True(t, tl.IsDuplicate(1, 5), "Client 1 should have transaction 5")
			assert.True(t, tl.IsDuplicate(1, 10), "Client 1 should have transaction 10")
			assert.True(t, tl.IsDuplicate(2, 5), "Client 2 should have transaction 5")
			assert.True(t, tl.IsDuplicate(2, 10), "Client 2 should have transaction 10")
			assert.True(t, tl.IsDuplicate(3, 5), "Client 3 should have transaction 5")

			// Cross-client isolation should be maintained
			assert.False(t, tl.IsDuplicate(1, 11), "Client 1 should not have transaction 11")
			assert.False(t, tl.IsDuplicate(2, 11), "Client 2 should not have transaction 11")
			assert.False(t, tl.IsDuplicate(3, 10), "Client 3 should not have transaction 10")
			assert.False(t, tl.IsDuplicate(4, 5), "Client 4 should not have any transactions")
		})
	})

	t.Run("Test Sequential Checkpoints With Different Clients", func(t *testing.T) {
		testWithOps(t, []Op{
			// Client 1 operations
			{receivedNormal: &receivedNormal{1, 1, []byte("client1_msg1")}},
			{acknowledged: &acknowledged{}},
			{receivedNormal: &receivedNormal{1, 2, []byte("client1_msg2")}},
			{acknowledged: &acknowledged{}},

			// First checkpoint
			{dump: &dump{}},

			// Client 2 operations
			{receivedNormal: &receivedNormal{2, 1, []byte("client2_msg1")}},
			{acknowledged: &acknowledged{}},
			{receivedNormal: &receivedNormal{2, 2, []byte("client2_msg2")}},
			{acknowledged: &acknowledged{}},

			// Second checkpoint
			{dump: &dump{}},

			// Client 3 operations
			{receivedNormal: &receivedNormal{3, 1, []byte("client3_msg1")}},
			{acknowledged: &acknowledged{}},
		}, func(tl TransactionLog) {
			// All clients should be correctly isolated
			assert.True(t, tl.IsDuplicate(1, 1), "Client 1 transaction 1 should be duplicate")
			assert.True(t, tl.IsDuplicate(1, 2), "Client 1 transaction 2 should be duplicate")
			assert.True(t, tl.IsDuplicate(2, 1), "Client 2 transaction 1 should be duplicate")
			assert.True(t, tl.IsDuplicate(2, 2), "Client 2 transaction 2 should be duplicate")
			assert.True(t, tl.IsDuplicate(3, 1), "Client 3 transaction 1 should be duplicate")

			// Verify no cross-contamination
			assert.False(t, tl.IsDuplicate(1, 3), "Client 1 should not have transaction 3")
			assert.False(t, tl.IsDuplicate(2, 3), "Client 2 should not have transaction 3")
			assert.False(t, tl.IsDuplicate(3, 2), "Client 3 should not have transaction 2")
		})
	})
}

type mockParent struct {
	ReceivedMessages map[uint64][]ReceivedMessage `json:"received_messages"`
	EofMessages      map[uint64]bool              `json:"eof_messages"`
	PrunedClients    map[uint64]bool              `json:"pruned_clients"`
	AckCount         int                          `json:"ack_count"`
	CheckpointData   []byte                       `json:"checkpoint_data"`
}

type ReceivedMessage struct {
	Id   uint64 `json:"id"`
	Data []byte `json:"data"`
}

func newMockParent() *mockParent {
	return &mockParent{
		ReceivedMessages: make(map[uint64][]ReceivedMessage),
		EofMessages:      make(map[uint64]bool),
		PrunedClients:    make(map[uint64]bool),
		AckCount:         0,
		CheckpointData:   nil,
	}
}

func (p *mockParent) Received(cid, id uint64, data []byte) error {
	if p.ReceivedMessages[cid] == nil {
		p.ReceivedMessages[cid] = make([]ReceivedMessage, 0)
	}
	p.ReceivedMessages[cid] = append(p.ReceivedMessages[cid], ReceivedMessage{Id: id, Data: data})
	return nil
}

func (p *mockParent) ReceivedEOF(cid uint64) error {
	p.EofMessages[cid] = true
	return nil
}

func (p *mockParent) ReceivedPrune(cid uint64) error {
	p.PrunedClients[cid] = true
	return nil
}

func (p *mockParent) Acknowledged() error {
	p.AckCount++
	return nil
}

func (p *mockParent) FromCheckpoint(data []byte) error {
	jsonStr := string(data)
	err := json.Unmarshal([]byte(jsonStr), p)
	if err != nil {
		return fmt.Errorf("Failed to unmarshal mockParent: %v", err)
	}
	return nil
}

func (p *mockParent) Dump() []byte {
	data, err := json.Marshal(p)
	if err != nil {
		panic(fmt.Errorf("Failed to marshal mockParent: %v", err))
	}
	return data
}

func testWithOps(t *testing.T, ops []Op, tests func(tl TransactionLog)) {
	// Create a mock file
	dirPath := t.TempDir()

	p := newMockParent()
	// Create a new TransactionLog instance
	tl, err := NewTransactionLogFromDir(dirPath, p)
	assert.NoError(t, err, "Expected no error when creating TransactionLog from directory")

	for _, op := range ops {
		var err error
		if op.receivedNormal != nil {
			err = tl.Received(op.receivedNormal.cid, op.receivedNormal.id, op.receivedNormal.data)
		} else if op.receivedEof != nil {
			err = tl.ReceivedEOF(op.receivedEof.cid)
		} else if op.acknowledged != nil {
			err = tl.Acknowledged()
		} else if op.dump != nil {
			// Cast to concrete type to access Dump method
			if concreteTL, ok := tl.(*transactionLog); ok {
				data := p.Dump()
				err = concreteTL.Dump(data)
			} else {
				t.Fatalf("Expected transactionLog type for dump operation")
			}
		}
		assert.NoError(t, err)
	}

	t.Logf("TransactionLog: %+v", tl)

	tests(tl)

	p2 := newMockParent()
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
