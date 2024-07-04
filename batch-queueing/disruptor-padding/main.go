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
)

type padding [cacheLine]byte

type RingBuffer struct {
	buffer          unsafe.Pointer
	bufferMask      int64
	bufferSize      int64
	cursor          *Sequence
	gatingSequences []*Sequence
	_padding0       padding
}

type Sequence struct {
	value    atomic.Int64
	_padding padding
}

func NewSequence(initial int64) *Sequence {
	s := &Sequence{}
	s.value.Store(initial)
	return s
}

// Add this method to the Sequence type
func (s *Sequence) IncrementAndGet() int64 {
	return s.value.Add(1)
}

func (s *Sequence) Get() int64 {
	return s.value.Load()
}

func (s *Sequence) Set(value int64) {
	s.value.Store(value)
}

func (s *Sequence) CompareAndSwap(expected, value int64) bool {
	return s.value.CompareAndSwap(expected, value)
}

func NewRingBuffer(bufferSize int64) *RingBuffer {
	if bufferSize < 1 || ((bufferSize & (bufferSize - 1)) != 0) {
		panic("bufferSize must be a positive power of 2")
	}
	rb := &RingBuffer{
		bufferSize: bufferSize,
		bufferMask: bufferSize - 1,
		cursor:     NewSequence(-1),
	}
	rb.buffer = unsafe.Pointer(&make([]int64, bufferSize)[0])
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

func (p *Producer) Publish(value int64) {
	sequence := p.rb.cursor.IncrementAndGet()
	*(*int64)(unsafe.Pointer(uintptr(p.rb.buffer) + uintptr(sequence&p.rb.bufferMask)*8)) = value
	p.waitStrategy.SignalAllWhenBlocking()
}

type Consumer struct {
	rb           *RingBuffer
	sequence     *Sequence
	handler      func(int64)
	waitStrategy WaitStrategy
	_padding0    padding
}

func NewConsumer(rb *RingBuffer, handler func(int64), waitStrategy WaitStrategy) *Consumer {
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
	for {
		availableSequence := c.waitStrategy.WaitFor(nextSequence, c.rb.cursor, c.sequence)
		for nextSequence <= availableSequence {
			event := *(*int64)(unsafe.Pointer(uintptr(c.rb.buffer) + uintptr(nextSequence&c.rb.bufferMask)*8))
			c.handler(event)
			nextSequence++
		}
		c.sequence.Set(availableSequence)
	}
}

type WaitStrategy interface {
	WaitFor(sequence int64, cursor *Sequence, dependents ...*Sequence) int64
	SignalAllWhenBlocking()
}

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

type YieldingWaitStrategy struct {
	spinTries int64
}

func NewYieldingWaitStrategy() *YieldingWaitStrategy {
	return &YieldingWaitStrategy{spinTries: 100}
}

func (y *YieldingWaitStrategy) WaitFor(sequence int64, cursor *Sequence, dependents ...*Sequence) int64 {
	var availableSequence int64
	counter := y.spinTries

	for {
		availableSequence = cursor.Get()
		if availableSequence >= sequence {
			return availableSequence
		}

		counter--
		if counter == 0 {
			runtime.Gosched()
			counter = y.spinTries
		}
	}
}

func (y *YieldingWaitStrategy) SignalAllWhenBlocking() {}

type SleepingWaitStrategy struct{}

func (SleepingWaitStrategy) WaitFor(sequence int64, cursor *Sequence, dependents ...*Sequence) int64 {
	for {
		availableSequence := cursor.Get()
		if availableSequence >= sequence {
			return availableSequence
		}
		time.Sleep(1 * time.Microsecond)
	}
}

func (SleepingWaitStrategy) SignalAllWhenBlocking() {}

var produced int64
var consumed int64

func runTest(name string, bufferSize int64, waitStrategy WaitStrategy, consumerCount int) {
	fmt.Printf("Running test: %s\n", name)
	ringBuffer := NewRingBuffer(bufferSize)
	producer := NewProducer(ringBuffer, waitStrategy)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	for i := 0; i < consumerCount; i++ {
		consumer := NewConsumer(ringBuffer, func(event int64) {
			atomic.AddInt64(&consumed, 1)
		}, waitStrategy)
		consumer.Start()
	}

	var running int32 = 1
	go func() {
		counter := int64(0)
		for atomic.LoadInt32(&running) == 1 {
			counter++
			producer.Publish(counter)
			atomic.AddInt64(&produced, 1)
		}
	}()

	<-ctx.Done()
	atomic.StoreInt32(&running, 0)

	time.Sleep(10 * time.Millisecond) // Allow final events to be consumed

	fmt.Printf("  Produced: %d\n", atomic.LoadInt64(&produced))
	fmt.Printf("  Consumed: %d\n", atomic.LoadInt64(&consumed))
	fmt.Printf("  Throughput: %.2f million ops/sec\n\n", float64(produced)/1000000)

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
		{"BusySpinWaitStrategy (1 consumer)", 1024 * 4, &BusySpinWaitStrategy{}, 1},
		{"BusySpinWaitStrategy (2 consumers)", 1024 * 4, &BusySpinWaitStrategy{}, 2},
		{"BusySpinWaitStrategy (numWorker consumers)", 1024 * 4, &BusySpinWaitStrategy{}, numWorkers},
		{"YieldingWaitStrategy (1 consumer)", 1024 * 4, NewYieldingWaitStrategy(), 1},
		{"YieldingWaitStrategy (2 consumers)", 1024 * 4, NewYieldingWaitStrategy(), 2},
		{"YieldingWaitStrategy (numWorkers consumers)", 1024 * 4, NewYieldingWaitStrategy(), numWorkers},
		{"SleepingWaitStrategy (1 consumer)", 1024 * 4, &SleepingWaitStrategy{}, 1},
		{"SleepingWaitStrategy (2 consumers)", 1024 * 4, &SleepingWaitStrategy{}, 2},
		{"SleepingWaitStrategy (numWorkers consumers)", 1024 * 4, &SleepingWaitStrategy{}, numWorkers},
	}

	for _, test := range tests {
		runTest(test.name, test.bufferSize, test.waitStrategy, test.consumerCount)
	}
}
