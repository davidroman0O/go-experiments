package main

import "sync/atomic"

// MultiRingBuffer manages multiple RingBuffers
type MultiRingBuffer struct {
	buffers []*RingBuffer
	next    int // For simple round-robin scheduling
}

func newMultiRingBuffer(numBuffers int, size int, quit chan struct{}) *MultiRingBuffer {
	buffers := make([]*RingBuffer, numBuffers)
	for i := range buffers {
		buffers[i] = newRingBuffer(size, quit)
	}
	return &MultiRingBuffer{buffers: buffers}
}

// Enqueue attempts to enqueue a message to one of the RingBuffers, using a simple round-robin strategy
func (mrb *MultiRingBuffer) enqueue(msg Message) bool {
	for i := 0; i < len(mrb.buffers); i++ {
		if mrb.buffers[mrb.next].enqueue(msg) {
			mrb.next = (mrb.next + 1) % len(mrb.buffers)
			return true
		}
		mrb.next = (mrb.next + 1) % len(mrb.buffers)
	}
	// All buffers are full
	return false
}

// EnqueueMany attempts to enqueue a batch of messages, distributing them across the buffers
func (mrb *MultiRingBuffer) enqueueMany(msgs []Message) bool {
	for _, msg := range msgs {
		if !mrb.enqueue(msg) {
			return false
		}
	}
	return true
}

const (
	numMessages      = 40000000        // Number of messages to process
	ringBufferSize   = 1024 * 1024 * 4 // Size of the ring buffer
	messageBatchSize = 1024 * 8        // Number of messages to process in a batch
)

// Message is the basic data structure representing a message
type Message struct {
	Value int
	Kill  bool // Flag to indicate a kill message
}

// RingBuffer is a lock-free ring buffer for message passing
type RingBuffer struct {
	buf  []Message
	head uint64
	tail uint64
	// tailCached uint64
	notifier chan struct{} // Added notifier channel
	quit     chan struct{} // Added notifier channel
}

func newRingBuffer(size int, quit chan struct{}) *RingBuffer {
	return &RingBuffer{
		buf:      make([]Message, size),
		head:     0,
		tail:     0,
		notifier: make(chan struct{}, 1), // Buffer of 1 to avoid blocking
		quit:     quit,
	}
}

func (rb *RingBuffer) close() {
	close(rb.notifier)
	close(rb.quit)
}

func (rb *RingBuffer) enqueue(msg Message) bool {
	tail := rb.tail
	head := atomic.LoadUint64(&rb.head)
	next := tail + 1
	if next-head >= uint64(len(rb.buf)) {
		return false
	}
	rb.buf[tail&uint64(len(rb.buf)-1)] = msg
	atomic.StoreUint64(&rb.tail, next)

	// Signal that a new message has been enqueued
	select {
	case rb.notifier <- struct{}{}:
	default:
		// Notifier already has a signal, don't block
	}

	return true
}

func (rb *RingBuffer) dequeue() (Message, bool) {
	tail := atomic.LoadUint64(&rb.tail)
	head := rb.head
	next := head + 1
	if tail == head {
		return Message{}, false
	}
	msg := rb.buf[head&uint64(len(rb.buf)-1)]
	atomic.StoreUint64(&rb.head, next)
	return msg, true
}

func (rb *RingBuffer) enqueueMany(msgs []Message) bool {
	length := uint64(len(msgs))
	tail := atomic.LoadUint64(&rb.tail)
	head := atomic.LoadUint64(&rb.head)

	// Check if there is enough space to enqueue all messages
	if tail+length-head > uint64(len(rb.buf)) {
		return false // Not enough space
	}

	for _, msg := range msgs {
		rb.buf[tail&uint64(len(rb.buf)-1)] = msg
		tail++
	}

	atomic.StoreUint64(&rb.tail, tail)

	// Signal that new messages have been enqueued
	select {
	case rb.notifier <- struct{}{}:
	default:
		// Notifier already has a signal, don't block
	}

	return true
}

// DequeueMany dequeues up to n messages from the RingBuffer
func (rb *RingBuffer) dequeueMany(n int) ([]Message, bool) {
	var msgs []Message
	tail := atomic.LoadUint64(&rb.tail)
	head := rb.head

	if tail == head {
		return nil, false
	}

	for i := 0; i < n; i++ {
		next := head + 1
		if tail == head {
			break
		}
		msg := rb.buf[head&uint64(len(rb.buf)-1)]
		msgs = append(msgs, msg)
		head = next
	}
	atomic.StoreUint64(&rb.head, head)
	return msgs, true
}

// Has checks if there are available messages in the RingBuffer
func (rb *RingBuffer) has() bool {
	tail := atomic.LoadUint64(&rb.tail)
	head := rb.head
	return tail != head
}
