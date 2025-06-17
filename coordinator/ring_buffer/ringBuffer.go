package ringBuffer

type RingBuffer struct {
	buf        []byte
	head, tail int
	size       int
	capacity   int
}

func NewRingBuffer(capacity int) *RingBuffer {
	return &RingBuffer{
		buf:      make([]byte, capacity),
		capacity: capacity,
	}
}

// Write data into the ring buffer
func (r *RingBuffer) Write(data []byte) int {
	n := len(data)
	if n > r.capacity-r.size {
		n = r.capacity - r.size // solo lo que entra
	}
	for i := 0; i < n; i++ {
		r.buf[r.tail] = data[i]
		r.tail = (r.tail + 1) % r.capacity
	}
	r.size += n
	return n
}

// Read up to len(dst) bytes
func (r *RingBuffer) Read(dst []byte) int {
	n := len(dst)
	if n > r.size {
		n = r.size
	}
	for i := 0; i < n; i++ {
		dst[i] = r.buf[r.head]
		r.head = (r.head + 1) % r.capacity
	}
	r.size -= n
	return n
}

// Consume advances the head pointer by n bytes and returns the actual number of bytes consumed
func (r *RingBuffer) Consume(n int) int {
	if n > r.size {
		n = r.size
	}
	r.head = (r.head + n) % r.capacity
	r.size -= n
	return n
}

// Peek reads n bytes from the buffer without consuming them
func (r *RingBuffer) Peek(n int) []byte {
	if n > r.size {
		n = r.size
	}
	if n == 0 {
		return []byte{}
	}

	result := make([]byte, n)
	head := r.head
	for i := 0; i < n; i++ {
		result[i] = r.buf[head]
		head = (head + 1) % r.capacity
	}
	return result
}
