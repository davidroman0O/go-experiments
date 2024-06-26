package main

import (
	"math/rand"
	"sync/atomic"
)

const (
	defaultMessageBatchSize = 1024 * 8        // Number of messages to process in a batch
	defaultRingBufferSize   = 1024 * 1024 * 4 // Size of the ring buffer
)

type RingBuffer[T any] struct {
	buf         []T
	head        uint64
	tail        uint64
	entryChan   chan []T
	notifier    chan struct{}
	subscribers []chan []T
	batchSize   int

	downsizeRequested uint32
	upsizeRequested   uint32
	downsizing        uint32
	upsizing          uint32
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
		entryChan: make(chan []T, c.size/c.batchSize),
		notifier:  make(chan struct{}, 1),
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
	head := atomic.LoadUint64(&rb.head)
	tail := atomic.LoadUint64(&rb.tail)

	for i := 0; i < n && tail != head; i++ {
		msg := rb.buf[head&uint64(len(rb.buf)-1)]
		msgs = append(msgs, msg)
		head++
	}
	atomic.StoreUint64(&rb.head, head)
	return msgs
}

func (rb *RingBuffer[T]) Capacity() int {
	return len(rb.buf)
}

func (rb *RingBuffer[T]) Length() int {
	head := atomic.LoadUint64(&rb.head)
	tail := atomic.LoadUint64(&rb.tail)
	return int(tail - head)
}

func (rb *RingBuffer[T]) Has() bool {
	head := atomic.LoadUint64(&rb.head)
	tail := atomic.LoadUint64(&rb.tail)
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

func (rb *RingBuffer[T]) Downsize() {
	atomic.StoreUint32(&rb.downsizeRequested, 1)
}

func (rb *RingBuffer[T]) Tick() {
	if atomic.LoadUint32(&rb.downsizing) == 1 || atomic.LoadUint32(&rb.upsizing) == 1 {
		return
	}

	if atomic.CompareAndSwapUint32(&rb.upsizeRequested, 1, 0) {
		atomic.StoreUint32(&rb.upsizing, 1)
		rb.resizeBuffer(rb.batchSize)
		atomic.StoreUint32(&rb.upsizing, 0)
		return
	}

	if atomic.CompareAndSwapUint32(&rb.downsizeRequested, 1, 0) {
		atomic.StoreUint32(&rb.downsizing, 1)
		rb.resizeBuffer(-rb.batchSize)
		atomic.StoreUint32(&rb.downsizing, 0)
		return
	}

	if len(rb.subscribers) == 0 {
		return
	}

	for {
		select {
		case msgs := <-rb.entryChan:
			rb.enqueueMessages(msgs)
		default:
			msgs := rb.dequeueMany(rb.batchSize)
			if len(msgs) == 0 {
				return
			}
			rb.dispatchMessages(msgs)
		}
	}
}

func (rb *RingBuffer[T]) resizeBuffer(change int) {
	head := atomic.LoadUint64(&rb.head)
	tail := atomic.LoadUint64(&rb.tail)
	numUnprocessed := tail - head

	newBufSize := len(rb.buf) + change
	if newBufSize < int(numUnprocessed) {
		newBufSize = int(numUnprocessed)
	}

	newBuf := make([]T, newBufSize)

	if head <= tail {
		copy(newBuf, rb.buf[head%uint64(len(rb.buf)):tail%uint64(len(rb.buf))])
	} else {
		part1Len := len(rb.buf) - int(head%uint64(len(rb.buf)))
		copy(newBuf, rb.buf[head%uint64(len(rb.buf)):])
		copy(newBuf[part1Len:], rb.buf[:tail%uint64(len(rb.buf))])
	}

	rb.buf = newBuf
	atomic.StoreUint64(&rb.head, 0)
	atomic.StoreUint64(&rb.tail, numUnprocessed)
}

func (rb *RingBuffer[T]) enqueueMessages(msgs []T) {
	tail := atomic.LoadUint64(&rb.tail)
	head := atomic.LoadUint64(&rb.head)

	// Check if there is enough space to enqueue all messages
	// We want to minimize the number of times we resize the buffer
	if tail+uint64(len(msgs)*2)-head > uint64(len(rb.buf)) {
		atomic.StoreUint32(&rb.upsizeRequested, 1)
	}

	for _, msg := range msgs {
		rb.buf[tail&uint64(len(rb.buf)-1)] = msg
		tail++
	}

	atomic.StoreUint64(&rb.tail, tail)
}

func (rb *RingBuffer[T]) dispatchMessages(msgs []T) {
	numSubscribers := len(rb.subscribers)
	msgParts := make([][]T, numSubscribers)
	for i, msg := range msgs {
		idx := i % numSubscribers
		msgParts[idx] = append(msgParts[idx], msg)
	}

	rand.Shuffle(len(rb.subscribers), func(i, j int) {
		rb.subscribers[i], rb.subscribers[j] = rb.subscribers[j], rb.subscribers[i]
	})

	for i, sub := range rb.subscribers {
		sub <- msgParts[i]
	}
}
