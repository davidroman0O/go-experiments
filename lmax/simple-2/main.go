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
	ringBufferSize = 1 << 20
	batchSize      = 1000
)

type padding [7]int64

type Sequence struct {
	value atomic.Int64
	_     padding
}

func NewSequence(initial int64) *Sequence {
	s := &Sequence{}
	s.value.Store(initial)
	return s
}

func (s *Sequence) Get() int64 {
	return s.value.Load()
}

func (s *Sequence) Set(value int64) {
	s.value.Store(value)
}

type Event struct {
	value int64
}

type RingBuffer struct {
	buffer      []Event
	mask        int64
	producerSeq *Sequence
	consumerSeq *Sequence
	_           padding
}

func NewRingBuffer(size int64) *RingBuffer {
	return &RingBuffer{
		buffer:      make([]Event, size),
		mask:        size - 1,
		producerSeq: NewSequence(-1),
		consumerSeq: NewSequence(-1),
	}
}

func (rb *RingBuffer) Get(sequence int64) *Event {
	return &rb.buffer[sequence&rb.mask]
}

func (rb *RingBuffer) Next(n int64) int64 {
	return rb.producerSeq.value.Add(n)
}

func (rb *RingBuffer) Publish(high int64) {
	rb.producerSeq.Set(high)
}

func (rb *RingBuffer) WaitFor(sequence int64) int64 {
	for {
		available := rb.producerSeq.Get()
		if available >= sequence {
			return available
		}
		runtime.Gosched()
	}
}

type EventHandler interface {
	OnEvent(event *Event, sequence int64, endOfBatch bool)
}

type BatchEventProcessor struct {
	ringBuffer   *RingBuffer
	eventHandler EventHandler
	sequence     *Sequence
	running      atomic.Bool
}

func NewBatchEventProcessor(ringBuffer *RingBuffer, eventHandler EventHandler) *BatchEventProcessor {
	return &BatchEventProcessor{
		ringBuffer:   ringBuffer,
		eventHandler: eventHandler,
		sequence:     NewSequence(-1),
	}
}

func (bep *BatchEventProcessor) Run() {
	if !bep.running.CompareAndSwap(false, true) {
		return
	}

	nextSequence := bep.sequence.Get() + 1
	for bep.running.Load() {
		availableSequence := bep.ringBuffer.WaitFor(nextSequence)

		for nextSequence <= availableSequence {
			event := bep.ringBuffer.Get(nextSequence)
			bep.eventHandler.OnEvent(event, nextSequence, nextSequence == availableSequence)
			nextSequence++
		}

		bep.sequence.Set(availableSequence)
	}
}

func (bep *BatchEventProcessor) Halt() {
	bep.running.Store(false)
}

type CountingEventHandler struct {
	count atomic.Int64
}

func (h *CountingEventHandler) OnEvent(event *Event, sequence int64, endOfBatch bool) {
	h.count.Add(1)
}

func producer(ctx context.Context, rb *RingBuffer, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		default:
			start := rb.Next(batchSize)
			for i := int64(0); i < batchSize; i++ {
				event := rb.Get(start - batchSize + 1 + i)
				atomic.StoreInt64((*int64)(unsafe.Pointer(&event.value)), time.Now().UnixNano())
			}
			rb.Publish(start)
		}
	}
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	rb := NewRingBuffer(ringBufferSize)
	handler := &CountingEventHandler{}
	processor := NewBatchEventProcessor(rb, handler)

	numProducers := runtime.NumCPU() / 2
	if numProducers < 1 {
		numProducers = 1
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(numProducers)

	go processor.Run()

	for i := 0; i < numProducers; i++ {
		go producer(ctx, rb, &wg)
	}

	start := time.Now()
	<-ctx.Done()
	wg.Wait()
	processor.Halt()

	elapsed := time.Since(start)
	totalEvents := handler.count.Load()
	eventsPerSecond := float64(totalEvents) / elapsed.Seconds()

	fmt.Printf("Test duration: %v\n", elapsed)
	fmt.Printf("Total events processed: %d\n", totalEvents)
	fmt.Printf("Events per second: %.2f\n", eventsPerSecond)
}
