package main

import (
	"context"
	"fmt"
	"runtime"
	"sync/atomic"
	"time"
)

// Produced: 14098055
// Consumed: 14098507
// 14M events in 1 second quite impressive

var produced int32
var consumed int32

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

	ringBuffer := NewRingBuffer(1024 * 4)
	waitStrategy := &BusySpinWaitStrategy{}

	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 1*time.Second)
	defer cancel()

	producer := NewProducer(ringBuffer, waitStrategy)

	consumer1 := NewConsumer(ringBuffer, func(event interface{}) {
		// fmt.Println("Consumer 1:", event)
		atomic.AddInt32(&consumed, 1)
	}, waitStrategy)

	// consumer2 := NewConsumer(ringBuffer, func(event interface{}) {
	// 	// fmt.Println("Consumer 2:", event)
	// 	atomic.AddInt32(&consumed, 1)
	// }, waitStrategy)

	consumer1.Start()
	// consumer2.Start()

	var running int32 = 1
	go func() {
		counter := 0
		for atomic.LoadInt32(&running) == 1 {
			counter++
			producer.Publish(counter)
			atomic.AddInt32(&produced, 1)
		}
		<-ctx.Done()
		atomic.StoreInt32(&running, 0)
	}()

	// Wait for consumers to finish processing
	// time.Sleep(1 * time.Second)
	<-ctx.Done()

	fmt.Println("Produced:", atomic.LoadInt32(&produced))
	fmt.Println("Consumed:", atomic.LoadInt32(&consumed))
}

const (
	cacheLine = 64 // Size of a cache line on most CPUs
)

// RingBuffer represents the main data structure
type RingBuffer struct {
	buffer          []interface{}
	bufferMask      int64
	cursor          *Sequence
	gatingSequences []*Sequence
}

// Sequence is used to track progress of both producers and consumers
type Sequence struct {
	value   atomic.Int64
	padding [cacheLine - 8]byte
}

// NewSequence creates a new Sequence
func NewSequence(initialValue int64) *Sequence {
	s := &Sequence{}
	s.value.Store(initialValue)
	return s
}

// Get returns the current value of the sequence
func (s *Sequence) Get() int64 {
	return s.value.Load()
}

// Set updates the value of the sequence
func (s *Sequence) Set(value int64) {
	s.value.Store(value)
}

// IncrementAndGet increments the sequence and returns the new value
func (s *Sequence) IncrementAndGet() int64 {
	return s.value.Add(1)
}

// NewRingBuffer creates a new RingBuffer
func NewRingBuffer(bufferSize int64) *RingBuffer {
	if bufferSize < 1 || ((bufferSize & (bufferSize - 1)) != 0) {
		panic("bufferSize must be a positive power of 2")
	}
	return &RingBuffer{
		buffer:     make([]interface{}, bufferSize),
		bufferMask: bufferSize - 1,
		cursor:     NewSequence(-1),
	}
}

// Producer represents an event producer
type Producer struct {
	ringBuffer   *RingBuffer
	waitStrategy WaitStrategy
}

// NewProducer creates a new Producer
func NewProducer(rb *RingBuffer, waitStrategy WaitStrategy) *Producer {
	return &Producer{
		ringBuffer:   rb,
		waitStrategy: waitStrategy,
	}
}

// Publish publishes an event to the ring buffer
func (p *Producer) Publish(event interface{}) {
	sequence := p.ringBuffer.cursor.IncrementAndGet()
	p.ringBuffer.buffer[sequence&p.ringBuffer.bufferMask] = event
	p.waitStrategy.SignalAllWhenBlocking()
}

// Consumer represents an event consumer
type Consumer struct {
	ringBuffer   *RingBuffer
	sequence     *Sequence
	handler      func(interface{})
	waitStrategy WaitStrategy
}

// NewConsumer creates a new Consumer
func NewConsumer(rb *RingBuffer, handler func(interface{}), waitStrategy WaitStrategy) *Consumer {
	return &Consumer{
		ringBuffer:   rb,
		sequence:     NewSequence(-1),
		handler:      handler,
		waitStrategy: waitStrategy,
	}
}

// Start begins consuming events
func (c *Consumer) Start() {
	go func() {
		nextSequence := c.sequence.Get() + 1
		for {
			availableSequence := c.waitStrategy.WaitFor(nextSequence, c.ringBuffer.cursor, c.sequence)

			for nextSequence <= availableSequence {
				event := c.ringBuffer.buffer[nextSequence&c.ringBuffer.bufferMask]
				c.handler(event)
				nextSequence++
			}

			c.sequence.Set(availableSequence)
		}
	}()
}

// WaitStrategy interface for different waiting implementations
type WaitStrategy interface {
	WaitFor(sequence int64, cursor *Sequence, dependents ...*Sequence) int64
	SignalAllWhenBlocking()
}

// BusySpinWaitStrategy implements a busy spin wait strategy
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

// SleepingWaitStrategy implements a sleeping wait strategy
type SleepingWaitStrategy struct{}

func (SleepingWaitStrategy) WaitFor(sequence int64, cursor *Sequence, dependents ...*Sequence) int64 {
	for {
		availableSequence := cursor.Get()
		if availableSequence >= sequence {
			return availableSequence
		}
		time.Sleep(1 * time.Millisecond)
	}
}

func (SleepingWaitStrategy) SignalAllWhenBlocking() {}
