package main

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

const (
	ringBufferSize = 1 << 20
	batchSize      = 100
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
	value atomic.Int64
}

type Sequencer interface {
	Next(n int64) int64
	Publish(lo, hi int64)
	IsAvailable(sequence int64) bool
	GetHighestPublishedSequence(nextSequence, availableSequence int64) int64
}

type SingleProducerSequencer struct {
	cursor       *Sequence
	bufferSize   int64
	waitStrategy WaitStrategy
	gatingSeqs   []*Sequence
}

func NewSingleProducerSequencer(bufferSize int64, waitStrategy WaitStrategy) *SingleProducerSequencer {
	return &SingleProducerSequencer{
		cursor:       NewSequence(-1),
		bufferSize:   bufferSize,
		waitStrategy: waitStrategy,
	}
}

func (s *SingleProducerSequencer) Next(n int64) int64 {
	nextValue := s.cursor.Get() + n
	wrapPoint := nextValue - s.bufferSize
	cachedGatingSequence := s.getMinimumSequence()

	if wrapPoint > cachedGatingSequence || cachedGatingSequence > nextValue {
		for {
			cachedGatingSequence = s.getMinimumSequence()
			if wrapPoint <= cachedGatingSequence {
				break
			}
			runtime.Gosched()
		}
	}

	return nextValue
}

func (s *SingleProducerSequencer) Publish(lo, hi int64) {
	s.cursor.Set(hi)
	s.waitStrategy.SignalAllWhenBlocking()
}

func (s *SingleProducerSequencer) IsAvailable(sequence int64) bool {
	return sequence <= s.cursor.Get()
}

func (s *SingleProducerSequencer) GetHighestPublishedSequence(lowerBound, availableSequence int64) int64 {
	return availableSequence
}

func (s *SingleProducerSequencer) getMinimumSequence() int64 {
	minimum := s.cursor.Get()

	for _, seq := range s.gatingSeqs {
		min := seq.Get()
		if min < minimum {
			minimum = min
		}
	}

	return minimum
}

type WaitStrategy interface {
	WaitFor(sequence int64, cursor *Sequence, dependentSequence *Sequence, barrier Sequencer) int64
	SignalAllWhenBlocking()
}

type BusySpinWaitStrategy struct{}

func (BusySpinWaitStrategy) WaitFor(sequence int64, cursor *Sequence, dependentSequence *Sequence, barrier Sequencer) int64 {
	for {
		if cursorValue := cursor.Get(); cursorValue >= sequence {
			return cursorValue
		}
		runtime.Gosched()
	}
}

func (BusySpinWaitStrategy) SignalAllWhenBlocking() {}

type RingBuffer struct {
	entries    []Event
	sequencer  Sequencer
	bufferSize int64
	mask       int64
}

func NewRingBuffer(sequencer Sequencer, bufferSize int64) *RingBuffer {
	return &RingBuffer{
		entries:    make([]Event, bufferSize),
		sequencer:  sequencer,
		bufferSize: bufferSize,
		mask:       bufferSize - 1,
	}
}

func (rb *RingBuffer) Get(sequence int64) *Event {
	return &rb.entries[sequence&rb.mask]
}

func (rb *RingBuffer) Next(n int64) int64 {
	return rb.sequencer.Next(n)
}

func (rb *RingBuffer) Publish(lo, hi int64) {
	rb.sequencer.Publish(lo, hi)
}

type EventHandler interface {
	OnEvent(event *Event, sequence int64, endOfBatch bool)
}

type BatchEventProcessor struct {
	dataProvider    *RingBuffer
	sequenceBarrier *SequenceBarrier
	eventHandler    EventHandler
	sequence        *Sequence
	running         atomic.Bool
}

func NewBatchEventProcessor(dataProvider *RingBuffer, sequenceBarrier *SequenceBarrier, eventHandler EventHandler) *BatchEventProcessor {
	return &BatchEventProcessor{
		dataProvider:    dataProvider,
		sequenceBarrier: sequenceBarrier,
		eventHandler:    eventHandler,
		sequence:        NewSequence(-1),
	}
}

func (bep *BatchEventProcessor) Run() {
	if !bep.running.CompareAndSwap(false, true) {
		return
	}

	nextSequence := bep.sequence.Get() + 1
	availableSequence := int64(-1)

	for bep.running.Load() {
		for {
			if availableSequence = bep.sequenceBarrier.WaitFor(nextSequence); availableSequence >= nextSequence {
				break
			}
			runtime.Gosched()
		}

		for nextSequence <= availableSequence {
			event := bep.dataProvider.Get(nextSequence)
			bep.eventHandler.OnEvent(event, nextSequence, nextSequence == availableSequence)
			nextSequence++
		}

		bep.sequence.Set(availableSequence)
	}
}

func (bep *BatchEventProcessor) Halt() {
	bep.running.Store(false)
}

func (bep *BatchEventProcessor) GetSequence() *Sequence {
	return bep.sequence
}

type SequenceBarrier struct {
	cursor            *Sequence
	waitStrategy      WaitStrategy
	dependentSequence *Sequence
	sequencer         Sequencer
}

func NewSequenceBarrier(sequencer Sequencer, waitStrategy WaitStrategy, dependentSequence *Sequence) *SequenceBarrier {
	return &SequenceBarrier{
		cursor:            sequencer.(*SingleProducerSequencer).cursor,
		waitStrategy:      waitStrategy,
		dependentSequence: dependentSequence,
		sequencer:         sequencer,
	}
}

func (sb *SequenceBarrier) WaitFor(sequence int64) int64 {
	return sb.waitStrategy.WaitFor(sequence, sb.cursor, sb.dependentSequence, sb.sequencer)
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
			high := rb.Next(batchSize)
			low := high - batchSize + 1
			for i := low; i <= high; i++ {
				event := rb.Get(i)
				event.value.Store(time.Now().UnixNano())
			}
			rb.Publish(low, high)
		}
	}
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	waitStrategy := BusySpinWaitStrategy{}
	sequencer := NewSingleProducerSequencer(ringBufferSize, waitStrategy)
	rb := NewRingBuffer(sequencer, ringBufferSize)

	handler := &CountingEventHandler{}
	sequenceBarrier := NewSequenceBarrier(sequencer, waitStrategy, nil)
	processor := NewBatchEventProcessor(rb, sequenceBarrier, handler)

	sequencer.gatingSeqs = append(sequencer.gatingSeqs, processor.GetSequence())

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
