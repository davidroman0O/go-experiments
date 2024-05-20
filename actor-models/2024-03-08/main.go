package main

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

/// This is by far the best one i got, i need to refactor that one to be used on gronos

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

// Worker function adjusted to use notifier for pausing and resuming
func Worker(id int, cb func(nb int32), wg *sync.WaitGroup, rb *RingBuffer, quit chan struct{}) {
	defer wg.Done()
	for {
		msgs, ok := rb.dequeueMany(messageBatchSize)
		if !ok {
			select {
			case <-rb.notifier: // Wait for a signal that new messages are available
				// fmt.Println("pause")
				runtime.Gosched()
				continue
			case <-quit: // Check if we're quitting
				fmt.Println("quit", id)
				return
			}
		}
		for _, msg := range msgs {
			if msg.Kill {
				// Received a kill message, exit gracefully
				return
			}
			// Simulate some work on the message
			_ = msg.Value * 2
		}
		cb(int32(len(msgs)))
	}
}

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

var constantCPU = (runtime.NumCPU() * runtime.NumCPU()) - 1
var numWorkers = runtime.NumCPU() - (runtime.NumCPU() / 2) // Number of worker goroutines

func main() {
	// Set GOMAXPROCS to utilize multiple CPU cores
	runtime.GOMAXPROCS(constantCPU)

	// Create a quit channel to signal workers to stop
	quit := make(chan struct{})

	// Create a MultiRingBuffer for message passing
	mrb := newMultiRingBuffer(numWorkers, ringBufferSize, quit)

	var total uint32 = 0

	cb := func(nb int32) {
		atomic.AddUint32(&total, uint32(nb))
	}

	// Create workers for each RingBuffer in MultiRingBuffer
	var wg sync.WaitGroup
	wg.Add(numWorkers)
	for i := 0; i < numWorkers; i++ {
		go Worker(i, cb, &wg, mrb.buffers[i], quit)
	}

	// Generate and enqueue messages
	start := time.Now()
	produced := 0
	for i := 0; i < numMessages; i += messageBatchSize {
		batch := make([]Message, min(messageBatchSize, numMessages-i))
		for j := 0; j < len(batch); j++ {
			batch[j] = Message{Value: i + j}
		}
		for !mrb.enqueueMany(batch) {
			// MultiRingBuffer is full, wait for workers to catch up
			time.Sleep(10 * time.Nanosecond)
		}
		produced += len(batch)
	}

	// Send a kill message to each worker by enqueueing it directly to each buffer
	for _, buffer := range mrb.buffers {
		killMsg := Message{Kill: true}
		for !buffer.enqueue(killMsg) {
			// If a buffer is full, wait a bit and try again
			time.Sleep(10 * time.Nanosecond)
		}
	}

	// Wait for all workers to finish
	wg.Wait()

	// Close the quit channel to signal workers to stop
	close(quit)

	elapsed := time.Since(start)

	fmt.Printf("Processed %d messages in %s - produced %d - total processed: %v \n", numMessages, elapsed, produced, total)
}

// Utility function to calculate the minimum of two integers
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
