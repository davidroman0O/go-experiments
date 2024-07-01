package main

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// Sequence represents a pointer to a particular point in the Disruptor sequence
type Sequence struct {
	value int64
}

func NewSequence(initialValue int64) *Sequence {
	return &Sequence{value: initialValue}
}

func (s *Sequence) Get() int64 {
	return atomic.LoadInt64(&s.value)
}

func (s *Sequence) Set(value int64) {
	atomic.StoreInt64(&s.value, value)
}

func (s *Sequence) IncrementAndGet() int64 {
	return atomic.AddInt64(&s.value, 1)
}

// WaitStrategy defines the interface for waiting on a cursor value
type WaitStrategy interface {
	WaitFor(sequence int64, cursor *Sequence, dependentSequence *Sequence) int64
	SignalAllWhenBlocking()
}

// BlockingWaitStrategy implements a blocking wait strategy using a condition variable
type BlockingWaitStrategy struct {
	mutex *sync.Mutex
	cond  *sync.Cond
}

func NewBlockingWaitStrategy() *BlockingWaitStrategy {
	m := &sync.Mutex{}
	return &BlockingWaitStrategy{
		mutex: m,
		cond:  sync.NewCond(m),
	}
}

func (s *BlockingWaitStrategy) WaitFor(sequence int64, cursor *Sequence, dependentSequence *Sequence) int64 {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	for cursor.Get() < sequence {
		s.cond.Wait()
	}

	return cursor.Get()
}

func (s *BlockingWaitStrategy) SignalAllWhenBlocking() {
	s.cond.Broadcast()
}

type padding [7]int64

type Event struct {
	value atomic.Value
}

type RingBuffer struct {
	buffer       []Event
	mask         int64
	size         int64
	cursor       *Sequence
	waitStrategy WaitStrategy
	_pad0        padding
}

func NewRingBuffer(size int64) *RingBuffer {
	rb := &RingBuffer{
		buffer:       make([]Event, size),
		mask:         size - 1,
		size:         size,
		cursor:       NewSequence(-1),
		waitStrategy: NewBlockingWaitStrategy(),
	}
	for i := range rb.buffer {
		rb.buffer[i].value.Store(int64(0))
	}
	return rb
}

func (rb *RingBuffer) Get(sequence int64) interface{} {
	return rb.buffer[sequence&rb.mask].value.Load()
}

func (rb *RingBuffer) Set(sequence int64, data interface{}) {
	rb.buffer[sequence&rb.mask].value.Store(data)
}

func (rb *RingBuffer) Next() int64 {
	return rb.cursor.IncrementAndGet()
}

func (rb *RingBuffer) Publish(sequence int64) {
	rb.cursor.Set(sequence)
	rb.waitStrategy.SignalAllWhenBlocking()
}

// SequenceBarrier coordinates claiming sequences for producers and consumers
type SequenceBarrier struct {
	cursor          *Sequence
	dependentCursor *Sequence
	waitStrategy    WaitStrategy
}

func NewSequenceBarrier(cursor *Sequence, dependentCursor *Sequence, waitStrategy WaitStrategy) *SequenceBarrier {
	return &SequenceBarrier{
		cursor:          cursor,
		dependentCursor: dependentCursor,
		waitStrategy:    waitStrategy,
	}
}

func (sb *SequenceBarrier) WaitFor(sequence int64) int64 {
	return sb.waitStrategy.WaitFor(sequence, sb.cursor, sb.dependentCursor)
}

// EventHandler defines the interface for processing events from the Disruptor
type EventHandler interface {
	OnEvent(event interface{}, sequence int64, endOfBatch bool)
}

// BatchEventProcessor consumes the events from the ring buffer
type BatchEventProcessor struct {
	ringBuffer      *RingBuffer
	sequenceBarrier *SequenceBarrier
	eventHandler    EventHandler
	sequence        *Sequence
	running         int32
}

func NewBatchEventProcessor(ringBuffer *RingBuffer, sequenceBarrier *SequenceBarrier, eventHandler EventHandler) *BatchEventProcessor {
	return &BatchEventProcessor{
		ringBuffer:      ringBuffer,
		sequenceBarrier: sequenceBarrier,
		eventHandler:    eventHandler,
		sequence:        NewSequence(-1),
		running:         0,
	}
}

func (bep *BatchEventProcessor) GetSequence() *Sequence {
	return bep.sequence
}

func (bep *BatchEventProcessor) Run() {
	if !atomic.CompareAndSwapInt32(&bep.running, 0, 1) {
		return
	}

	nextSequence := bep.sequence.Get() + 1
	for {
		availableSequence := bep.sequenceBarrier.WaitFor(nextSequence)

		for nextSequence <= availableSequence {
			event := bep.ringBuffer.Get(nextSequence)
			bep.eventHandler.OnEvent(event, nextSequence, nextSequence == availableSequence)
			nextSequence++
		}

		bep.sequence.Set(availableSequence)

		if atomic.LoadInt32(&bep.running) == 0 {
			break
		}
	}
}

func (bep *BatchEventProcessor) Halt() {
	atomic.StoreInt32(&bep.running, 0)
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

	// Create a new RingBuffer
	ringBuffer := NewRingBuffer(1024)

	// Create a SequenceBarrier
	sequenceBarrier := NewSequenceBarrier(ringBuffer.cursor, nil, ringBuffer.waitStrategy)

	// Create an EventHandler that counts processed events
	eventHandler := &CountingEventHandler{count: 0}

	// Create a BatchEventProcessor
	processor := NewBatchEventProcessor(ringBuffer, sequenceBarrier, eventHandler)

	// Start the BatchEventProcessor in a goroutine
	go processor.Run()

	// Prepare for timing
	start := time.Now()
	duration := 5 * time.Second

	// Create a context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	// Create a WaitGroup to wait for the publisher goroutine
	var wg sync.WaitGroup
	wg.Add(1)

	// Start publishing events in a separate goroutine
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			default:
				sequence := ringBuffer.Next()
				ringBuffer.Set(sequence, time.Now().UnixNano())
				ringBuffer.Publish(sequence)
			}
		}
	}()

	// Wait for the context to be done (timeout reached)
	<-ctx.Done()

	// Wait for the publisher goroutine to finish
	wg.Wait()

	// Wait a bit to allow processing of remaining events
	time.Sleep(100 * time.Millisecond)

	// Halt the processor
	processor.Halt()

	// Calculate results
	elapsed := time.Since(start)
	totalEvents := eventHandler.count
	eventsPerSecond := float64(totalEvents) / elapsed.Seconds()

	fmt.Printf("Test duration: %v\n", elapsed)
	fmt.Printf("Total events processed: %d\n", totalEvents)
	fmt.Printf("Events per second: %.2f\n", eventsPerSecond)
}

// CountingEventHandler is an EventHandler that counts processed events
type CountingEventHandler struct {
	count int64
}

func (h *CountingEventHandler) OnEvent(event interface{}, sequence int64, endOfBatch bool) {
	if _, ok := event.(int64); ok {
		atomic.AddInt64(&h.count, 1)
	}
}
