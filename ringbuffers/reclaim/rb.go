package main

import (
	"context"
	"math/rand"
	"sync/atomic"
	"time"
)

///
///	General concept of that RingBuffer:
///
///             +--------------------------------------------------------+
///             |                                                        |
///             |                                                        |
/// +----------------------+      +----------+                           |
/// |           |          |      |          |                           |
/// |           |          | +--> |          |                           |
/// |           |          |      |          |                           |
/// +----------------------+      |          |                           |
///             |                 |          |                           |
///             |       +         |          |                           |
///             |       |         |          |                           |
///             |       |         |          |                           |
///             |       |         |          |         +---------------+ |
///             |       |         +----------+ Tail    |   consumer    +--->
///             |       |         |----------|         +---------------+ |
///             |       |         |--|Data|--|         +---------------+ |
///             |       |         |----------|  +----->+   consumer    +--->
///             |       |         |----------|         +---------------+ |
///             |       v         +----------+ Head    +---------------+ |
///             |    +-----+      |          |         |   consumer    +--->
///             |    |     |      |          |         +---------------+ |
///             |    |     | +--> |          |                           |
///             |    +-----+      |          |                           |
///             |                 |          |                           |
///             |                 |          |                           |
///             |                 |          |       Ticker              |
///             |                 |          |                           |
///             |                 +----------+                           |
///             |                                                        |
///             +--------------------------------------------------------+
///
/// The ring buffer is a fixed-size buffer with a single writer and multiple readers.
/// The writer enqueues messages into the buffer and the readers dequeue messages from the buffer.
/// A clock tick is used to regulate the rate of messages being enqueued and dequeued.
/// There is a backpressure mechanism that will slow down the writer if the buffer is full or not enough messages have been dequeued.
/// The buffer can be resized dynamically to accommodate more messages.
/// The developer can request the buffer to be resized by calling the Downsize() method to reduce it's memory footprint.

const (
	defaultMessageBatchSize = 1024 * 8        // Number of messages to process in a batch
	defaultRingBufferSize   = 1024 * 1024 * 4 // Size of the ring buffer
)

type RingBuffer[T any] struct {
	context   context.Context
	buf       []T
	head      uint64
	tail      uint64
	entryChan chan []T

	subscribers []chan []T
	batchSize   int

	downsizeRequested uint32
	upsizeRequested   uint32
	downsizing        uint32
	upsizing          uint32

	enqueueCount uint64
	dequeueCount uint64
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

func NewRingBuffer[T any](ctx context.Context, opts ...Option) *RingBuffer[T] {
	c := ringBufferConfig{
		size:      defaultRingBufferSize,
		batchSize: defaultMessageBatchSize,
	}
	for _, opt := range opts {
		opt(&c)
	}
	return &RingBuffer[T]{
		context:   ctx,
		buf:       make([]T, c.size),
		head:      0,
		batchSize: c.batchSize,
		tail:      0,
		entryChan: make(chan []T, c.size/c.batchSize),
	}
}

func (rb *RingBuffer[T]) Close() {
	close(rb.entryChan)
}

// Potentially enqueue your messages into the RingBuffer but return a channel that will be closed when the messages are actually enqueued.
// In order to regulate the rate of messages being enqueued, the function will wait until enough messages have been dequeued before enqueuing the next batch.
// That waiting behaviour might not be triggered if the buffer got enough space to enqueue all messages.
// We could have wait and hide the behaviour from the user but we decided to expose it to give more control to the user. Most of your use cases should need to wait.
func (rb *RingBuffer[T]) Enqueue(msgs []T) <-chan struct{} {
	done := make(chan struct{})
	// TODO: we need to change the waiting condition to optimize the write/read
	go func() {
		ticker := time.NewTicker(1 * time.Nanosecond)
		for {
			select {
			case <-rb.context.Done():
				done <- struct{}{}
				close(done)
				ticker.Stop()
				return
			case <-ticker.C:
				// TODO: optimize this
				if atomic.LoadUint64(&rb.dequeueCount) >= atomic.LoadUint64(&rb.enqueueCount) {
					done <- struct{}{}
					close(done)
					ticker.Stop()
					return
				}
				// runtime.Gosched()
			}
		}
	}()
	go func() {
		<-done
		rb.entryChan <- msgs
		atomic.AddUint64(&rb.enqueueCount, uint64(len(msgs)))
	}()
	return done
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
	atomic.AddUint64(&rb.dequeueCount, uint64(len(msgs)))
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

func (rb *RingBuffer[T]) NormalizedDelta() float64 {
	enqueueCount := atomic.LoadUint64(&rb.enqueueCount)
	dequeueCount := atomic.LoadUint64(&rb.dequeueCount)
	delta := int64(enqueueCount) - int64(dequeueCount)
	return float64(delta) / float64(rb.batchSize)
}

func (rb *RingBuffer[T]) Delta() int64 {
	enqueueCount := atomic.LoadUint64(&rb.enqueueCount)
	dequeueCount := atomic.LoadUint64(&rb.dequeueCount)
	return int64(enqueueCount) - int64(dequeueCount)
}
