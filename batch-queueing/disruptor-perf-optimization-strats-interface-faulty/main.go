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

type RingBuffer struct {
	buffer               unsafe.Pointer
	bufferMask           int64
	_padding0            padding
	cursor               *Sequence
	_padding1            padding
	gatingSequences      []*Sequence
	_padding2            padding
	elementSize          uintptr
	lastProducedSequence int64
	lastConsumedSequence int64
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
	return int64(atomic.LoadUint64(&s.value))
}

//go:nosplit
func (s *Sequence) Set(value int64) {
	atomic.StoreUint64(&s.value, uint64(value))
}

//go:nosplit
func (s *Sequence) IncrementAndGet() int64 {
	return int64(atomic.AddUint64(&s.value, 1)) - 1
}

//go:nosplit
func (rb *RingBuffer) ClaimNextSequences(n int64) (int64, int64) {
	start := atomic.AddInt64(&rb.lastProducedSequence, n) - n
	return start, start + n - 1
}

//go:nosplit
func (rb *RingBuffer) MarkConsumed(sequence int64) {
	for {
		last := atomic.LoadInt64(&rb.lastConsumedSequence)
		if sequence <= last {
			return
		}
		if atomic.CompareAndSwapInt64(&rb.lastConsumedSequence, last, sequence) {
			return
		}
	}
}

func NewRingBuffer(bufferSize int64, sampleElement interface{}) *RingBuffer {
	if bufferSize < 1 || ((bufferSize & (bufferSize - 1)) != 0) {
		panic("bufferSize must be a positive power of 2")
	}
	rb := &RingBuffer{
		bufferMask:  bufferSize - 1,
		cursor:      NewSequence(-1),
		elementSize: unsafe.Sizeof(sampleElement),
	}

	buffer := make([]interface{}, bufferSize)
	rb.buffer = unsafe.Pointer(&buffer[0])

	// Pre-touch pages
	for i := int64(0); i < bufferSize; i += 4096 / int64(rb.elementSize) {
		buffer[i] = nil
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
func (p *Producer) PublishBatch(values []interface{}) {
	n := int64(len(values))
	start, end := p.rb.ClaimNextSequences(n)

	for i := int64(0); i < n; i++ {
		dst := (*interface{})(unsafe.Pointer(uintptr(p.rb.buffer) + uintptr((start+i)&p.rb.bufferMask)*p.rb.elementSize))
		*dst = values[i]
	}

	p.rb.cursor.Set(end)
	p.waitStrategy.SignalAllWhenBlocking()
}

// func (p *Producer) PublishBatch(values []interface{}) {
// 	n := int64(len(values))
// 	sequence := p.rb.cursor.IncrementAndGet()
// 	highSequence := sequence + n - 1

// 	for i := int64(0); i < n; i++ {
// 		dst := (*interface{})(unsafe.Pointer(uintptr(p.rb.buffer) + uintptr((sequence+i)&p.rb.bufferMask)*p.rb.elementSize))
// 		*dst = values[i]
// 	}

// 	p.rb.cursor.Set(highSequence)
// 	p.waitStrategy.SignalAllWhenBlocking()
// }

type WaitStrategy interface {
	WaitFor(sequence int64, cursor *Sequence, dependents ...*Sequence) int64
	SignalAllWhenBlocking()
}

type Consumer struct {
	rb           *RingBuffer
	sequence     *Sequence
	handler      func([]interface{})
	waitStrategy WaitStrategy
	_padding0    padding
}

func NewConsumer(rb *RingBuffer, handler func([]interface{}), waitStrategy WaitStrategy) *Consumer {
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

// func (c *Consumer) run() {
// 	nextSequence := c.sequence.Get() + 1
// 	eventsBatch := make([]interface{}, consumerBatchSize)

// 	for {
// 		availableSequence := c.waitStrategy.WaitFor(nextSequence, c.rb.cursor, c.sequence)

// 		for nextSequence <= availableSequence {
// 			batchEnd := nextSequence + consumerBatchSize
// 			if batchEnd > availableSequence+1 {
// 				batchEnd = availableSequence + 1
// 			}

// 			for i := int64(0); nextSequence < batchEnd; i++ {
// 				src := (*interface{})(unsafe.Pointer(uintptr(c.rb.buffer) + uintptr(nextSequence&c.rb.bufferMask)*c.rb.elementSize))
// 				eventsBatch[i] = *src
// 				nextSequence++
// 			}

// 			c.handler(eventsBatch[:batchEnd-nextSequence+consumerBatchSize])
// 		}

//			c.sequence.Set(availableSequence)
//		}
//	}

// func (c *Consumer) run() {
// 	nextSequence := c.sequence.Get() + 1
// 	eventsBatch := make([]interface{}, consumerBatchSize)

// 	for {
// 		availableSequence := c.waitStrategy.WaitFor(nextSequence, c.rb.cursor, c.sequence)

// 		for nextSequence <= availableSequence {
// 			batchEnd := nextSequence + consumerBatchSize
// 			if batchEnd > availableSequence+1 {
// 				batchEnd = availableSequence + 1
// 			}

// 			batchSize := batchEnd - nextSequence
// 			for i := int64(0); i < batchSize; i++ {
// 				src := (*interface{})(unsafe.Pointer(uintptr(c.rb.buffer) + uintptr((nextSequence+i)&c.rb.bufferMask)*c.rb.elementSize))
// 				eventsBatch[i] = *src
// 			}

// 			c.handler(eventsBatch[:batchSize])
// 			c.rb.MarkConsumed(nextSequence + batchSize - 1)
// 			nextSequence = batchEnd
// 		}

// 		c.sequence.Set(availableSequence)
// 	}
// }

func (c *Consumer) run() {
	nextSequence := c.sequence.Get() + 1
	eventsBatch := make([]interface{}, consumerBatchSize)

	for {
		availableSequence := c.waitStrategy.WaitFor(nextSequence, c.rb.cursor, c.sequence)

		for nextSequence <= availableSequence {
			batchEnd := nextSequence + consumerBatchSize
			if batchEnd > availableSequence+1 {
				batchEnd = availableSequence + 1
			}

			batchSize := batchEnd - nextSequence
			for i := int64(0); i < batchSize; i++ {
				src := (*interface{})(unsafe.Pointer(uintptr(c.rb.buffer) + uintptr((nextSequence+i)&c.rb.bufferMask)*c.rb.elementSize))
				eventsBatch[i] = *src
			}

			c.handler(eventsBatch[:batchSize])

			// Use CompareAndSwap to ensure only one consumer processes each sequence
			for seq := nextSequence; seq < batchEnd; seq++ {
				for {
					lastConsumed := atomic.LoadInt64(&c.rb.lastConsumedSequence)
					if seq <= lastConsumed {
						break // This sequence has already been consumed
					}
					if atomic.CompareAndSwapInt64(&c.rb.lastConsumedSequence, lastConsumed, seq) {
						break // Successfully marked as consumed
					}
				}
			}

			nextSequence = batchEnd
		}

		c.sequence.Set(availableSequence)
	}
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

type Msg struct{}

var produced uint64
var consumed uint64

func runTest(name string, bufferSize int64, waitStrategy WaitStrategy, consumerCount int, sample interface{}) {
	fmt.Printf("Running test: %s\n", name)
	ringBuffer := NewRingBuffer(bufferSize, sample)
	producer := NewProducer(ringBuffer, waitStrategy)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var wg sync.WaitGroup

	// Start consumers
	for i := 0; i < consumerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			consumer := NewConsumer(ringBuffer, func(events []interface{}) {
				atomic.AddUint64(&consumed, uint64(len(events)))
			}, waitStrategy)
			consumer.Start()
			<-ctx.Done()
		}()
	}

	// Start producer
	wg.Add(1)
	go func() {
		defer wg.Done()
		batch := make([]interface{}, producerBatchSize)
		for {
			select {
			case <-ctx.Done():
				return
			default:
				for i := range batch {
					batch[i] = Msg{}
				}
				producer.PublishBatch(batch)
				atomic.AddUint64(&produced, uint64(len(batch)))
			}
		}
	}()

	// Wait for the test duration
	<-ctx.Done()

	// Wait for all goroutines to finish
	wg.Wait()

	// Give a small grace period for any final updates
	time.Sleep(100 * time.Millisecond)

	producedFinal := atomic.LoadUint64(&produced)
	consumedFinal := atomic.LoadUint64(&consumed)

	fmt.Printf("  Produced: %d\n", producedFinal)
	fmt.Printf("  Consumed: %d\n", consumedFinal)
	fmt.Printf("  Last Produced Sequence: %d\n", ringBuffer.lastProducedSequence)
	fmt.Printf("  Last Consumed Sequence: %d\n", ringBuffer.lastConsumedSequence)
	fmt.Printf("  Throughput: %.2f million ops/sec\n\n", float64(producedFinal)/10.0)

	// Reset for next test
	atomic.StoreUint64(&produced, 0)
	atomic.StoreUint64(&consumed, 0)
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

	for _, test := range tests {
		runTest(test.name, test.bufferSize, test.waitStrategy, test.consumerCount, Msg{})
	}
}
