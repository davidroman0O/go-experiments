package main

import (
	"math/rand"
	"sync/atomic"
)

const (
	defaultMessageBatchSize = 1024 * 8        // Number of messages to process in a batch
	defaultRingBufferSize   = 1024 * 1024 * 4 // Size of the ring buffer
)

// RingBuffer is a lock-free ring buffer for message passing
type RingBuffer[T any] struct {
	buf         []T
	head        uint64
	tail        uint64
	entryChan   chan []T
	notifier    chan struct{} // notify when a new messages is enqueued
	subscribers []chan []T    // TODO implement
	batchSize   int

	resizeRequested uint32
	resizing        uint32
}

type ringBufferConfig struct {
	size      int
	batchSize int
}

type Option func(*ringBufferConfig)

func WithSize(size int) Option {
	return func(c *ringBufferConfig) {
		c.size = size
	}
}

func WithBatchSize(batchSize int) Option {
	return func(c *ringBufferConfig) {
		c.batchSize = batchSize
	}
}

func NewRingBuffer[T any](opts ...Option) *RingBuffer[T] {
	c := ringBufferConfig{
		size:      defaultRingBufferSize,
		batchSize: defaultMessageBatchSize,
	}
	for _, opt := range opts {
		opt(&c)
	}
	return &RingBuffer[T]{
		buf:       make([]T, c.size),
		head:      0,
		batchSize: c.batchSize,
		tail:      0,
		entryChan: make(chan []T, c.size/c.batchSize), // Buffer based on size/batchSize
		notifier:  make(chan struct{}, 1),             // Buffer of 1 to avoid blocking
	}
}

func (rb *RingBuffer[T]) Close() {
	close(rb.notifier)
	close(rb.entryChan)
}

func (rb *RingBuffer[T]) Enqueue(msgs []T) {
	rb.entryChan <- msgs
}

func (rb *RingBuffer[T]) dequeueMany(n int) []T {
	var msgs []T
	tail := atomic.LoadUint64(&rb.tail)
	head := rb.head

	for i := 0; i < n && tail != head; i++ {
		msg := rb.buf[head&uint64(len(rb.buf)-1)]
		msgs = append(msgs, msg)
		head++
	}
	atomic.StoreUint64(&rb.head, head)
	return msgs
}

// Capacity returns the total size of the buffer
func (rb *RingBuffer[T]) Capacity() int {
	return len(rb.buf)
}

// Length returns the number of elements currently in the buffer
func (rb *RingBuffer[T]) Length() int {
	head := atomic.LoadUint64(&rb.head)
	tail := atomic.LoadUint64(&rb.tail)
	return int(tail - head)
}

// Has checks if there are available messages in the RingBuffer
func (rb *RingBuffer[T]) Has() bool {
	tail := atomic.LoadUint64(&rb.tail)
	head := rb.head
	return tail != head
}

func (rb *RingBuffer[T]) Subscribe(subscriber chan []T) {
	rb.subscribers = append(rb.subscribers, subscriber)
}

func (rb *RingBuffer[T]) Unsubscribe(subscriber chan []T) {
	for i, sub := range rb.subscribers {
		if sub == subscriber {
			rb.subscribers = append(rb.subscribers[:i], rb.subscribers[i+1:]...)
			return
		}
	}
}

// Downsize requests a resize operation to be performed during the next Tick
func (rb *RingBuffer[T]) Downsize() {
	atomic.StoreUint32(&rb.resizeRequested, 1)
}

// performResize shrinks the buffer to clean up unused indexes and recomputes head and tail
func (rb *RingBuffer[T]) performResize() {
	// Lock the buffer
	atomic.StoreUint32(&rb.resizing, 1)
	defer atomic.StoreUint32(&rb.resizing, 0)

	// Calculate the number of unprocessed messages
	head := atomic.LoadUint64(&rb.head)
	tail := atomic.LoadUint64(&rb.tail)
	numUnprocessed := tail - head

	// Create a new buffer with the size of unprocessed messages
	newBuf := make([]T, numUnprocessed)
	for i := uint64(0); i < numUnprocessed; i++ {
		newBuf[i] = rb.buf[(head+i)&uint64(len(rb.buf)-1)]
	}

	// Update buffer and reset head and tail
	rb.buf = newBuf
	atomic.StoreUint64(&rb.head, 0)
	atomic.StoreUint64(&rb.tail, numUnprocessed)
}

// When triggered by a clock, the ring buffer will dispatch messages to subscribers
func (rb *RingBuffer[T]) Tick() {
	// Check if a resize operation has been requested
	if atomic.LoadUint32(&rb.resizing) == 1 {
		return
	}

	if atomic.CompareAndSwapUint32(&rb.resizeRequested, 1, 0) {
		atomic.StoreUint32(&rb.resizing, 1)
		rb.performResize()
		atomic.StoreUint32(&rb.resizing, 0)
		return
	}

	if len(rb.subscribers) == 0 {
		return
	}

	has := true
	// Process entries from the entry channel
	for has {
		select {
		case msgs := <-rb.entryChan:
			tail := atomic.LoadUint64(&rb.tail)
			head := atomic.LoadUint64(&rb.head)

			// Check if there is enough space to enqueue all messages
			if tail+uint64(len(msgs))-head > uint64(len(rb.buf)) {
				continue // Not enough space
			}

			for _, msg := range msgs {
				rb.buf[tail&uint64(len(rb.buf)-1)] = msg
				tail++
			}

			atomic.StoreUint64(&rb.tail, tail)
		default:
			has = false
		}
	}

	// Dispatch messages to subscribers
	msgs := rb.dequeueMany(rb.batchSize)

	// Split messages into unique parts
	numSubscribers := len(rb.subscribers)
	msgParts := make([][]T, numSubscribers)
	for i, msg := range msgs {
		idx := i % numSubscribers
		msgParts[idx] = append(msgParts[idx], msg)
	}

	// Randomize the order of subscribers
	rand.Shuffle(len(rb.subscribers), func(i, j int) {
		rb.subscribers[i], rb.subscribers[j] = rb.subscribers[j], rb.subscribers[i]
	})

	for i, sub := range rb.subscribers {
		part := msgParts[i]
		sub <- part
	}
}
