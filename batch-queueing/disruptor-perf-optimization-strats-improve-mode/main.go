package main

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

const (
	cacheLine         = 64
	producerBatchSize = 8192
	consumerBatchSize = 8192
)

type padding [cacheLine]byte

type RingBufferMode int

const (
	DispatchMode RingBufferMode = iota
	BroadcastMode
)

// RingBuffer with potential for NUMA-aware allocation
type RingBuffer struct {
	buffer          unsafe.Pointer
	bufferMask      int64
	_padding0       padding
	cursor          *Sequence
	_padding1       padding
	gatingSequences []*Sequence
	_padding2       padding
	mode            RingBufferMode
}

type Sequence struct {
	value    uint64
	_padding padding
}

//go:nosplit
func NewSequence(initial int64) *Sequence {
	return &Sequence{value: uint64(initial)}
}

//go:nosplit
func (s *Sequence) Get() int64 {
	return atomic.LoadInt64((*int64)(unsafe.Pointer(&s.value)))
}

//go:nosplit
func (s *Sequence) Set(value int64) {
	atomic.StoreInt64((*int64)(unsafe.Pointer(&s.value)), value)
}

//go:nosplit
func (s *Sequence) IncrementAndGet() int64 {
	return atomic.AddInt64((*int64)(unsafe.Pointer(&s.value)), 1) - 1
}

func NewRingBuffer(bufferSize int64, mode RingBufferMode) *RingBuffer {
	if bufferSize < 1 || ((bufferSize & (bufferSize - 1)) != 0) {
		panic("bufferSize must be a positive power of 2")
	}
	rb := &RingBuffer{
		bufferMask: bufferSize - 1,
		cursor:     NewSequence(-1),
		mode:       mode,
	}

	// Ideally, use NUMA-aware allocation here
	buffer := make([]int64, bufferSize)
	rb.buffer = unsafe.Pointer(&buffer[0])

	// Pre-touch pages
	for i := int64(0); i < bufferSize; i += 4096 / 8 {
		buffer[i] = 0
	}

	return rb
}

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

//go:nosplit
func (p *Producer) PublishBatch(values []int64) {
	n := int64(len(values))
	sequence := p.rb.cursor.IncrementAndGet()
	highSequence := sequence + n - 1

	src := unsafe.Pointer(&values[0])
	for i := int64(0); i < n; i++ {
		dst := unsafe.Pointer(uintptr(p.rb.buffer) + uintptr((sequence+i)&p.rb.bufferMask)*8)
		atomic.StoreInt64((*int64)(dst), *(*int64)(unsafe.Pointer(uintptr(src) + uintptr(i)*8)))
	}

	atomic.StoreInt64((*int64)(unsafe.Pointer(&p.rb.cursor.value)), highSequence)
	p.waitStrategy.SignalAllWhenBlocking()
}

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

//go:nosplit
func (s *Sequence) CompareAndSwap(old, new int64) bool {
	return atomic.CompareAndSwapInt64((*int64)(unsafe.Pointer(&s.value)), old, new)
}

func (c *Consumer) run() {
	nextSequence := c.sequence.Get() + 1
	eventsBatch := make([]int64, consumerBatchSize)

	for {
		availableSequence := c.waitStrategy.WaitFor(nextSequence, c.rb.cursor, c.sequence)

		if c.rb.mode == DispatchMode {
			// In dispatch mode, we need to compete for sequences
			for nextSequence <= availableSequence {
				if c.sequence.CompareAndSwap(nextSequence-1, nextSequence) {
					src := unsafe.Pointer(uintptr(c.rb.buffer) + uintptr(nextSequence&c.rb.bufferMask)*8)
					event := atomic.LoadInt64((*int64)(src))
					c.handler([]int64{event})
				}
				nextSequence++
			}
		} else {
			// Broadcast mode (existing logic)
			for nextSequence <= availableSequence {
				batchEnd := nextSequence + consumerBatchSize
				if batchEnd > availableSequence+1 {
					batchEnd = availableSequence + 1
				}

				for i := int64(0); nextSequence < batchEnd; i++ {
					src := unsafe.Pointer(uintptr(c.rb.buffer) + uintptr(nextSequence&c.rb.bufferMask)*8)
					eventsBatch[i] = atomic.LoadInt64((*int64)(src))
					nextSequence++
				}

				c.handler(eventsBatch[:batchEnd-nextSequence+consumerBatchSize])
			}
		}

		c.sequence.Set(availableSequence)
	}
}

type WaitStrategy interface {
	WaitFor(sequence int64, cursor *Sequence, dependents ...*Sequence) int64
	SignalAllWhenBlocking()
}

// BusySpinWaitStrategy for ultra-low latency
type BusySpinWaitStrategy struct{}

func (BusySpinWaitStrategy) WaitFor(sequence int64, cursor *Sequence, dependents ...*Sequence) int64 {
	for {
		availableSequence := cursor.Get()
		if availableSequence >= sequence {
			return availableSequence
		}
		runtime.Gosched()
	}
}

func (BusySpinWaitStrategy) SignalAllWhenBlocking() {}

// YieldingWaitStrategy with adaptive spinning
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

// SleepingWaitStrategy for CPU-friendly waiting
type SleepingWaitStrategy struct {
	retries int32
}

func NewSleepingWaitStrategy() *SleepingWaitStrategy {
	return &SleepingWaitStrategy{retries: 200}
}

func (s *SleepingWaitStrategy) WaitFor(sequence int64, cursor *Sequence, dependents ...*Sequence) int64 {
	counter := s.retries

	for {
		availableSequence := cursor.Get()
		if availableSequence >= sequence {
			return availableSequence
		}

		counter--
		if counter <= 0 {
			time.Sleep(1 * time.Millisecond)
		} else if counter < 100 {
			runtime.Gosched()
		}
	}
}

func (s *SleepingWaitStrategy) SignalAllWhenBlocking() {}

// BlockingWaitStrategy using conditional variables
type BlockingWaitStrategy struct {
	mutex sync.Mutex
	cond  *sync.Cond
}

func NewBlockingWaitStrategy() *BlockingWaitStrategy {
	s := &BlockingWaitStrategy{}
	s.cond = sync.NewCond(&s.mutex)
	return s
}

func (b *BlockingWaitStrategy) WaitFor(sequence int64, cursor *Sequence, dependents ...*Sequence) int64 {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	for {
		availableSequence := cursor.Get()
		if availableSequence >= sequence {
			return availableSequence
		}
		b.cond.Wait()
	}
}

func (b *BlockingWaitStrategy) SignalAllWhenBlocking() {
	b.cond.Broadcast()
}

var produced int64
var consumed int64

func modeToString(mode RingBufferMode) string {
	if mode == DispatchMode {
		return "Dispatch"
	}
	return "Broadcast"
}

// TODO: need a way to shutdown and wait for consumers to finish
func runTest(name string, bufferSize int64, waitStrategy WaitStrategy, consumerCount int, mode RingBufferMode) {
	fmt.Printf("Running test: %s (%s mode)\n", name, modeToString(mode))
	ringBuffer := NewRingBuffer(bufferSize, mode)

	producer := NewProducer(ringBuffer, waitStrategy)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
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
	fmt.Printf("  Throughput: %.2f million ops/sec\n\n", float64(produced)/10000000)

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
		{"BusySpinWaitStrategy (1 consumer)", 1024 * 1024 * 64, &BusySpinWaitStrategy{}, 1},
		{"BusySpinWaitStrategy (2 consumers)", 1024 * 1024 * 64, &BusySpinWaitStrategy{}, 2},
		{"YieldingWaitStrategy (1 consumer)", 1024 * 1024 * 64, NewYieldingWaitStrategy(), 1},
		{"YieldingWaitStrategy (2 consumers)", 1024 * 1024 * 64, NewYieldingWaitStrategy(), 2},
		{"SleepingWaitStrategy (1 consumer)", 1024 * 1024 * 64, NewSleepingWaitStrategy(), 1},
		{"SleepingWaitStrategy (2 consumers)", 1024 * 1024 * 64, NewSleepingWaitStrategy(), 2},
		{"BlockingWaitStrategy (1 consumer)", 1024 * 1024 * 64, NewBlockingWaitStrategy(), 1},
		{"BlockingWaitStrategy (2 consumers)", 1024 * 1024 * 64, NewBlockingWaitStrategy(), 2},
	}

	modes := []RingBufferMode{DispatchMode, BroadcastMode}

	for _, mode := range modes {
		for _, test := range tests {
			runTest(test.name, test.bufferSize, test.waitStrategy, test.consumerCount, mode)
		}
	}
}
