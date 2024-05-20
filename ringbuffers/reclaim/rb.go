package main

import (
	"sync/atomic"
)

const (
	numMessages           = 40000000        // Number of messages to process
	messageBatchSize      = 1024 * 8        // Number of messages to process in a batch
	defaultRingBufferSize = 1024 * 1024 * 4 // Size of the ring buffer
)

// RingBuffer is a lock-free ring buffer for message passing
type RingBuffer[T any] struct {
	buf      []T
	head     uint64
	tail     uint64
	notifier chan struct{} // notify when a new messages is enqueued
}

type ringBufferConfig struct {
	size int
}

type Option func(*ringBufferConfig)

func WithSize(size int) Option {
	return func(c *ringBufferConfig) {
		c.size = size
	}
}

func NewRingBuffer[T any](opts ...Option) *RingBuffer[T] {
	c := ringBufferConfig{
		size: defaultRingBufferSize,
	}
	for _, opt := range opts {
		opt(&c)
	}
	return &RingBuffer[T]{
		buf:      make([]T, c.size),
		head:     0,
		tail:     0,
		notifier: make(chan struct{}, 1), // Buffer of 1 to avoid blocking
	}
}

func (rb *RingBuffer[T]) Close() {
	close(rb.notifier)
}

func (rb *RingBuffer[T]) Enqueue(msg T) bool {
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

func (rb *RingBuffer[T]) Dequeue() (*T, bool) {
	tail := atomic.LoadUint64(&rb.tail)
	head := rb.head
	next := head + 1
	if tail == head {
		return nil, false
	}
	msg := rb.buf[head&uint64(len(rb.buf)-1)] // just retrieve (compiler don't like but don't care)
	atomic.StoreUint64(&rb.head, next)        // while incrementing head
	return &msg, true
}

func (rb *RingBuffer[T]) EnqueueMany(msgs []T) bool {
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
func (rb *RingBuffer[T]) DequeueMany(n int) ([]T, bool) {
	var msgs []T
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
	if rb.Has() {
		rb.notifier <- struct{}{}
	}
	return msgs, true
}

// Has checks if there are available messages in the RingBuffer
func (rb *RingBuffer[T]) Has() bool {
	tail := atomic.LoadUint64(&rb.tail)
	head := rb.head
	return tail != head
}
