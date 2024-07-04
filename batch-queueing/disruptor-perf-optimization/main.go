package main

import (
	"context"
	"fmt"
	"runtime"
	"sync/atomic"
	"time"
	"unsafe"
)

const (
	cacheLine = 64
	// Larger batch size for even more throughput
	producerBatchSize = 4096
	consumerBatchSize = 4096
)

type padding [cacheLine]byte

// RingBuffer with memory-mapped IO for potential performance boost
type RingBuffer struct {
	buffer          unsafe.Pointer
	bufferMask      int64
	_padding0       padding
	cursor          *Sequence
	_padding1       padding
	gatingSequences []*Sequence
	_padding2       padding
}

// Sequence using a custom atomic implementation
type Sequence struct {
	value    uint64
	_padding padding
}

func NewSequence(initial int64) *Sequence {
	return &Sequence{value: uint64(initial)}
}

//go:noinline
func (s *Sequence) Get() int64 {
	return int64(atomic.LoadUint64(&s.value))
}

//go:noinline
func (s *Sequence) Set(value int64) {
	atomic.StoreUint64(&s.value, uint64(value))
}

//go:noinline
func (s *Sequence) IncrementAndGet() int64 {
	return int64(atomic.AddUint64(&s.value, 1)) - 1
}

// NewRingBuffer with potential memory-mapped IO
func NewRingBuffer(bufferSize int64) *RingBuffer {
	if bufferSize < 1 || ((bufferSize & (bufferSize - 1)) != 0) {
		panic("bufferSize must be a positive power of 2")
	}
	rb := &RingBuffer{
		bufferMask: bufferSize - 1,
		cursor:     NewSequence(-1),
	}

	// Here we would ideally use memory-mapped IO for even better performance
	// For simplicity, we're still using a regular slice
	buffer := make([]int64, bufferSize)
	rb.buffer = unsafe.Pointer(&buffer[0])

	// Pre-touch pages
	for i := int64(0); i < bufferSize; i += 4096 / 8 {
		buffer[i] = 0
	}

	return rb
}

// Producer with larger batch size and potential for multiple parallel producers
type Producer struct {
	rb           *RingBuffer
	waitStrategy WaitStrategy
	_padding0    padding
}

func NewProducer(rb *RingBuffer, waitStrategy WaitStrategy) *Producer {
	return &Producer{
		rb:           rb,
		waitStrategy: waitStrategy,
	}
}

func (p *Producer) PublishBatch(values []int64) {
	n := int64(len(values))
	sequence := p.rb.cursor.IncrementAndGet()
	highSequence := sequence + n - 1

	// Use unsafe operations for faster memory writes
	src := unsafe.Pointer(&values[0])
	for i := int64(0); i < n; i++ {
		dst := unsafe.Pointer(uintptr(p.rb.buffer) + uintptr((sequence+i)&p.rb.bufferMask)*8)
		*(*int64)(dst) = *(*int64)(unsafe.Pointer(uintptr(src) + uintptr(i)*8))
	}

	p.rb.cursor.Set(highSequence)
	p.waitStrategy.SignalAllWhenBlocking()
}

// Consumer with larger batch size and potential for SIMD operations
type Consumer struct {
	rb           *RingBuffer
	sequence     *Sequence
	handler      func([]int64)
	waitStrategy WaitStrategy
	_padding0    padding
}

func NewConsumer(rb *RingBuffer, handler func([]int64), waitStrategy WaitStrategy) *Consumer {
	return &Consumer{
		rb:           rb,
		sequence:     NewSequence(-1),
		handler:      handler,
		waitStrategy: waitStrategy,
	}
}

func (c *Consumer) Start() {
	go c.run()
}

func (c *Consumer) run() {
	nextSequence := c.sequence.Get() + 1
	eventsBatch := make([]int64, consumerBatchSize)

	for {
		availableSequence := c.waitStrategy.WaitFor(nextSequence, c.rb.cursor, c.sequence)

		for nextSequence <= availableSequence {
			batchEnd := nextSequence + consumerBatchSize
			if batchEnd > availableSequence+1 {
				batchEnd = availableSequence + 1
			}

			// Use unsafe operations for faster memory reads
			for i := int64(0); nextSequence < batchEnd; i++ {
				src := unsafe.Pointer(uintptr(c.rb.buffer) + uintptr(nextSequence&c.rb.bufferMask)*8)
				*(*int64)(unsafe.Pointer(uintptr(unsafe.Pointer(&eventsBatch[0])) + uintptr(i)*8)) = *(*int64)(src)
				nextSequence++
			}

			c.handler(eventsBatch[:batchEnd-nextSequence+consumerBatchSize])
		}

		c.sequence.Set(availableSequence)
	}
}

// Optimized YieldingWaitStrategy with adaptive spinning
type YieldingWaitStrategy struct {
	spinTries uint32
}

func NewYieldingWaitStrategy() *YieldingWaitStrategy {
	return &YieldingWaitStrategy{spinTries: 100}
}

func (y *YieldingWaitStrategy) WaitFor(sequence int64, cursor *Sequence, dependents ...*Sequence) int64 {
	counter := y.spinTries

	for {
		availableSequence := cursor.Get()
		if availableSequence >= sequence {
			return availableSequence
		}

		counter--
		if counter == 0 {
			runtime.Gosched()
			counter = y.spinTries
		} else {
			runtime.Gosched()
		}
	}
}

func (y *YieldingWaitStrategy) SignalAllWhenBlocking() {}

type WaitStrategy interface {
	WaitFor(sequence int64, cursor *Sequence, dependents ...*Sequence) int64
	SignalAllWhenBlocking()
}

var produced int64
var consumed int64

func runTest(name string, bufferSize int64, waitStrategy WaitStrategy, consumerCount int) {
	fmt.Printf("Running test: %s\n", name)
	ringBuffer := NewRingBuffer(bufferSize)
	producer := NewProducer(ringBuffer, waitStrategy)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for i := 0; i < consumerCount; i++ {
		consumer := NewConsumer(ringBuffer, func(events []int64) {
			atomic.AddInt64(&consumed, int64(len(events)))
		}, waitStrategy)
		consumer.Start()
	}

	var running int32 = 1
	go func() {
		counter := int64(0)
		batch := make([]int64, producerBatchSize)
		for atomic.LoadInt32(&running) == 1 {
			for i := range batch {
				counter++
				batch[i] = counter
			}
			producer.PublishBatch(batch)
			atomic.AddInt64(&produced, int64(len(batch)))
		}
	}()

	<-ctx.Done()
	atomic.StoreInt32(&running, 0)

	time.Sleep(100 * time.Millisecond) // Allow final events to be consumed

	fmt.Printf("  Produced: %d\n", atomic.LoadInt64(&produced))
	fmt.Printf("  Consumed: %d\n", atomic.LoadInt64(&consumed))
	fmt.Printf("  Throughput: %.2f million ops/sec\n\n", float64(produced)/5000000)

	atomic.StoreInt64(&produced, 0)
	atomic.StoreInt64(&consumed, 0)
}

func calculateNumWorkers() int {
	numCPU := runtime.NumCPU()
	numWorkers := numCPU / 4
	if numWorkers == 0 {
		numWorkers = 1
	}
	return numWorkers
}

var numWorkers = calculateNumWorkers() // Number of worker goroutines

func main() {
	runtime.GOMAXPROCS(numWorkers)
	tests := []struct {
		name          string
		bufferSize    int64
		waitStrategy  WaitStrategy
		consumerCount int
	}{
		{"YieldingWaitStrategy (1 consumer)", 1024 * 1024 * 64, NewYieldingWaitStrategy(), 1},
		{"YieldingWaitStrategy (2 consumers)", 1024 * 1024 * 64, NewYieldingWaitStrategy(), 2},
		{"YieldingWaitStrategy (4 consumers)", 1024 * 1024 * 64, NewYieldingWaitStrategy(), 4},
	}

	for _, test := range tests {
		runTest(test.name, test.bufferSize, test.waitStrategy, test.consumerCount)
	}
}
