package main

import (
	"fmt"
	"runtime"
	"sync/atomic"
	"time"
	"unsafe"
)

const (
	ringBufferSize    = 1 << 24
	producerBatchSize = 4096
	consumerBatchSize = 4096
	runDuration       = 5 * time.Second
	cacheLine         = 64
)

type padding [cacheLine - 8]byte

type Sequence struct {
	value atomic.Int64
	_     padding
}

func NewSequence(initial int64) *Sequence {
	s := &Sequence{}
	s.value.Store(initial)
	return s
}

type Event struct {
	value int64
}

type RingBuffer struct {
	buffer     []Event
	bufferMask int64
	cursor     *Sequence
	_          padding
}

func NewRingBuffer(size int64) *RingBuffer {
	return &RingBuffer{
		buffer:     make([]Event, size),
		bufferMask: size - 1,
		cursor:     NewSequence(-1),
	}
}

type Producer struct {
	rb *RingBuffer
	_  padding
}

func NewProducer(rb *RingBuffer) *Producer {
	return &Producer{rb: rb}
}

func (p *Producer) Next(n int64) int64 {
	return p.rb.cursor.value.Add(n)
}

func (p *Producer) Publish(lo, hi int64) {
	p.rb.cursor.value.Store(hi)
}

type Consumer struct {
	rb       *RingBuffer
	sequence *Sequence
	_        padding
}

func NewConsumer(rb *RingBuffer) *Consumer {
	return &Consumer{
		rb:       rb,
		sequence: NewSequence(-1),
	}
}

func (c *Consumer) WaitFor(sequence int64) int64 {
	for {
		availableSequence := c.rb.cursor.value.Load()
		if availableSequence >= sequence {
			return availableSequence
		}
		runtime.Gosched()
	}
}

func producer(p *Producer, done <-chan struct{}) {
	values := make([]int64, producerBatchSize)
	for {
		select {
		case <-done:
			return
		default:
			high := p.Next(producerBatchSize)
			low := high - producerBatchSize + 1
			for i := range values {
				values[i] = time.Now().UnixNano()
			}
			for i, v := range values {
				event := p.rb.buffer[(low+int64(i))&p.rb.bufferMask]
				*(*int64)(unsafe.Pointer(&event.value)) = v
			}
			p.Publish(low, high)
		}
	}
}

func consumer(c *Consumer, count *int64, done <-chan struct{}) {
	nextSequence := c.sequence.value.Load() + 1
	for {
		select {
		case <-done:
			return
		default:
			availableSequence := c.WaitFor(nextSequence)
			for nextSequence <= availableSequence {
				event := c.rb.buffer[nextSequence&c.rb.bufferMask]
				_ = *(*int64)(unsafe.Pointer(&event.value))
				nextSequence++
				atomic.AddInt64(count, 1)
			}
			c.sequence.value.Store(availableSequence)
		}
	}
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	rb := NewRingBuffer(ringBufferSize)
	p := NewProducer(rb)

	numProducers := runtime.NumCPU() / 2
	if numProducers < 1 {
		numProducers = 1
	}
	numConsumers := runtime.NumCPU() / 2
	if numConsumers < 1 {
		numConsumers = 1
	}

	done := make(chan struct{})
	var count int64

	for i := 0; i < numProducers; i++ {
		go producer(p, done)
	}

	consumers := make([]*Consumer, numConsumers)
	for i := 0; i < numConsumers; i++ {
		consumers[i] = NewConsumer(rb)
		go consumer(consumers[i], &count, done)
	}

	time.Sleep(runDuration)
	close(done)

	// Allow consumers to finish processing
	time.Sleep(10 * time.Millisecond)

	eventsPerSecond := float64(atomic.LoadInt64(&count)) / runDuration.Seconds()

	fmt.Printf("Test duration: %v\n", runDuration)
	fmt.Printf("Total events processed: %d\n", count)
	fmt.Printf("Events per second: %.2f\n", eventsPerSecond)
}
