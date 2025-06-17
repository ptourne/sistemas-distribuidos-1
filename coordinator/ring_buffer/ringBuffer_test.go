package ringBuffer

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewRingBuffer(t *testing.T) {
	capacity := 10
	rb := NewRingBuffer(capacity)

	assert.Equal(t, capacity, rb.capacity)
	assert.Equal(t, capacity, len(rb.buf))
	assert.Equal(t, 0, rb.size)
}

func TestRingBufferWrite(t *testing.T) {
	rb := NewRingBuffer(5)

	// Test writing within capacity
	data := []byte{1, 2, 3}
	n := rb.Write(data)
	assert.Equal(t, 3, n)
	assert.Equal(t, 3, rb.size)
	assert.Equal(t, []byte{1, 2, 3, 0, 0}, rb.buf)

	// Test writing beyond capacity
	data2 := []byte{4, 5, 6, 7}
	n = rb.Write(data2)
	assert.Equal(t, 2, n)
	assert.Equal(t, 5, rb.size)
	assert.Equal(t, []byte{1, 2, 3, 4, 5}, rb.buf)
}

func TestRingBufferRead(t *testing.T) {
	rb := NewRingBuffer(5)

	// Write some data
	data := []byte{1, 2, 3, 4, 5}
	rb.Write(data)

	// Test reading within available data
	dst := make([]byte, 3)
	n := rb.Read(dst)
	assert.Equal(t, 3, n)
	assert.Equal(t, 2, rb.size)
	assert.Equal(t, []byte{1, 2, 3}, dst)

	// Test reading remaining data
	dst2 := make([]byte, 3)
	n = rb.Read(dst2)
	assert.Equal(t, 2, n)
	assert.Equal(t, 0, rb.size)
	assert.Equal(t, []byte{4, 5, 0}, dst2)
}

func TestRingBufferCircular(t *testing.T) {
	rb := NewRingBuffer(3)

	// Write until full
	data := []byte{1, 2, 3}
	n := rb.Write(data)
	assert.Equal(t, 3, n)
	assert.Equal(t, 3, rb.size)
	assert.Equal(t, []byte{1, 2, 3}, rb.buf)

	// Read some data
	dst := make([]byte, 2)
	n = rb.Read(dst)
	assert.Equal(t, 2, n)
	assert.Equal(t, 1, rb.size)
	assert.Equal(t, []byte{1, 2}, dst)

	// Write more data (should wrap around)
	data2 := []byte{4, 5}
	n = rb.Write(data2)
	assert.Equal(t, 2, n)
	assert.Equal(t, 3, rb.size)
	assert.Equal(t, []byte{4, 5, 3}, rb.buf)

	// Read data
	dst2 := make([]byte, 2)
	n = rb.Read(dst2)
	assert.Equal(t, 2, n)
	assert.Equal(t, 1, rb.size)
	assert.Equal(t, []byte{3, 4}, dst2)

	// read all data
	dst3 := make([]byte, 1)
	n = rb.Read(dst3)
	assert.Equal(t, 1, n)
	assert.Equal(t, 0, rb.size)
	assert.Equal(t, []byte{5}, dst3)
}

func TestRingBufferEmptyRead(t *testing.T) {
	rb := NewRingBuffer(5)

	// Try to read from empty buffer
	dst := make([]byte, 3)
	n := rb.Read(dst)
	assert.Equal(t, 0, n)
	assert.Equal(t, 0, rb.size)
	assert.Equal(t, []byte{0, 0, 0, 0, 0}, rb.buf)
}

func TestRingBufferConsume(t *testing.T) {
	rb := NewRingBuffer(5)

	// Write some data
	data := []byte{1, 2, 3, 4, 5}
	rb.Write(data)

	// Test consume within available data
	n := rb.Consume(3)
	assert.Equal(t, 3, n)
	assert.Equal(t, 2, rb.size)
	assert.Equal(t, []byte{1, 2, 3, 4, 5}, rb.buf)

	// Test consume remaining data
	n = rb.Consume(2)
	assert.Equal(t, 2, n)
	assert.Equal(t, rb.size, 0)
	assert.Equal(t, rb.buf, []byte{1, 2, 3, 4, 5})
}

func TestRingBufferCircular2(t *testing.T) {
	rb := NewRingBuffer(3)

	// Write until full
	data := []byte{1, 2, 3}
	n := rb.Write(data)
	assert.Equal(t, 3, n)
	assert.Equal(t, 3, rb.size)
	assert.Equal(t, []byte{1, 2, 3}, rb.buf)

	// Read some data
	n2 := rb.Consume(2)
	assert.Equal(t, 2, n2)
	assert.Equal(t, 1, rb.size)
	assert.Equal(t, []byte{1, 2, 3}, rb.buf)

	// Write more data (should wrap around)
	data2 := []byte{4, 5}
	n = rb.Write(data2)
	assert.Equal(t, 2, n)
	assert.Equal(t, 3, rb.size)
	assert.Equal(t, []byte{4, 5, 3}, rb.buf)

	// Read data
	n2 = rb.Consume(2)
	assert.Equal(t, 2, n2)
	assert.Equal(t, 1, rb.size)
	assert.Equal(t, []byte{4, 5, 3}, rb.buf)

	// read all data
	n2 = rb.Consume(1)
	assert.Equal(t, 1, n2)
	assert.Equal(t, 0, rb.size)
	assert.Equal(t, []byte{4, 5, 3}, rb.buf)
}

func TestRingBufferEmptyConsume(t *testing.T) {
	rb := NewRingBuffer(5)

	// Try to consume from empty buffer
	n := rb.Consume(3)
	assert.Equal(t, 0, n)
	assert.Equal(t, 0, rb.size)
	assert.Equal(t, []byte{0, 0, 0, 0, 0}, rb.buf)
}

func TestRingBufferPeek(t *testing.T) {
	rb := NewRingBuffer(5)

	// Write some data
	data := []byte{1, 2, 3, 4, 5}
	rb.Write(data)

	// Test peeking within available data
	result := rb.Peek(3)
	assert.Equal(t, 5, rb.size) // size should not change
	assert.Equal(t, []byte{1, 2, 3}, result)

	// Peek again to verify buffer state hasn't changed
	result2 := rb.Peek(3)
	assert.Equal(t, 5, rb.size) // size should still not change
	assert.Equal(t, []byte{1, 2, 3}, result2)

	// Test peeking more than available
	result3 := rb.Peek(7)
	assert.Equal(t, 5, rb.size)
	assert.Equal(t, []byte{1, 2, 3, 4, 5}, result3)

	//consume
	n := rb.Consume(3)
	assert.Equal(t, 3, n)
	assert.Equal(t, 2, rb.size)
	assert.Equal(t, []byte{1, 2, 3, 4, 5}, rb.buf)

	//peek again
	result4 := rb.Peek(3)
	assert.Equal(t, 2, rb.size)
	assert.Equal(t, []byte{4, 5}, result4)
}

func TestRingBufferPeekCircular(t *testing.T) {
	rb := NewRingBuffer(3)

	// Write until full
	data := []byte{1, 2, 3}
	rb.Write(data)

	// Read some data to create space
	rb.Consume(2)

	// Write more data (should wrap around)
	data2 := []byte{4, 5}
	rb.Write(data2)

	// Peek all data
	result := rb.Peek(3)
	assert.Equal(t, 3, rb.size)
	assert.Equal(t, []byte{3, 4, 5}, result)
}

func TestRingBufferPeekEmpty(t *testing.T) {
	rb := NewRingBuffer(5)

	// Try to peek from empty buffer
	result := rb.Peek(3)
	assert.Equal(t, 0, rb.size)
	assert.Equal(t, []byte{}, result)
}
