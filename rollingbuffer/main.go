package main

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

const defaultThroughput = 300

type Scheduler interface {
	Schedule(fn func())
	Throughput() int
}

type goScheduler int

func (gs goScheduler) Schedule(fn func()) {
	go fn()
}

func (gs goScheduler) Throughput() int {
	return int(gs)
}

func NewScheduler(throughput int) Scheduler {
	return goScheduler(throughput)
}

type ChannelState int32

const (
	ChannelOpen ChannelState = iota
	ChannelClosed
)

type ProducerChannel struct {
	ch          chan<- interface{}
	state       *atomic.Int32
	workerCount int
}

type ConsumerChannel struct {
	ch          <-chan interface{}
	closed      chan struct{}
	workerCount int
}

type RollingBuffer struct {
	producerChannels []ProducerChannel
	consumerChannels []ConsumerChannel
	ringBuffers      []*RingBuffer[interface{}]
	currentBuffer    *RingBuffer[interface{}]
	bufferSize       int
	consumerSize     int
	mu               sync.Mutex
	scheduler        Scheduler
}

func NewRollingBuffer(bufferSize, consumerSize int) *RollingBuffer {
	rb := &RollingBuffer{
		bufferSize:   bufferSize,
		consumerSize: consumerSize,
		scheduler:    NewScheduler(defaultThroughput),
	}
	rb.currentBuffer = New[interface{}](int64(bufferSize))
	rb.ringBuffers = append(rb.ringBuffers, rb.currentBuffer)
	return rb
}

func (rb *RollingBuffer) NewProducer(workerCount int) ProducerChannel {
	ch := make(chan interface{}, rb.bufferSize)
	pc := ProducerChannel{
		ch:          ch,
		state:       &atomic.Int32{},
		workerCount: workerCount,
	}
	rb.mu.Lock()
	rb.producerChannels = append(rb.producerChannels, pc)
	rb.mu.Unlock()
	for i := 0; i < workerCount; i++ {
		rb.scheduler.Schedule(func() { rb.producerWorker(ch) })
	}
	return pc
}

func (rb *RollingBuffer) NewConsumer(workerCount int) ConsumerChannel {
	ch := make(chan interface{}, rb.consumerSize)
	closed := make(chan struct{})
	cc := ConsumerChannel{ch: ch, closed: closed, workerCount: workerCount}
	rb.mu.Lock()
	rb.consumerChannels = append(rb.consumerChannels, cc)
	rb.mu.Unlock()
	for i := 0; i < workerCount; i++ {
		rb.scheduler.Schedule(func() { rb.consumerWorker(ch, closed) })
	}
	return cc
}

func (rb *RollingBuffer) producerWorker(ch <-chan interface{}) {
	i, t := 0, rb.scheduler.Throughput()
	for data := range ch {
		if i > t {
			i = 0
			runtime.Gosched()
		}
		i++

		rb.mu.Lock()
		if rb.currentBuffer == nil {
			rb.mu.Unlock()
			return
		}
		if rb.currentBuffer.IsFull() {
			rb.currentBuffer = New[interface{}](int64(rb.bufferSize))
			rb.ringBuffers = append(rb.ringBuffers, rb.currentBuffer)
		}
		rb.currentBuffer.Push(data)
		rb.mu.Unlock()
	}
}

func (rb *RollingBuffer) consumerWorker(ch chan<- interface{}, closed <-chan struct{}) {
	i, t := 0, rb.scheduler.Throughput()
	for {
		if i > t {
			i = 0
			runtime.Gosched()
		}
		i++

		select {
		case <-closed:
			return
		default:
			data, ok := rb.read()
			if ok {
				select {
				case ch <- data:
					// Data sent successfully
				case <-closed:
					return
				}
			} else {
				time.Sleep(10 * time.Millisecond)
			}
		}
	}
}

func (rb *RollingBuffer) read() (interface{}, bool) {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	for i, buffer := range rb.ringBuffers {
		if !buffer.IsEmpty() {
			data, _ := buffer.Pop()
			if buffer.IsEmpty() {
				rb.ringBuffers = append(rb.ringBuffers[:i], rb.ringBuffers[i+1:]...)
			}
			return data, true
		}
	}
	return nil, false
}

func (rb *RollingBuffer) Close(wait bool) {
	fmt.Println("Starting Close function...")
	rb.mu.Lock()

	fmt.Println("Closing producer channels...")
	for i, pc := range rb.producerChannels {
		pc.state.Store(int32(ChannelClosed))
		close(pc.ch)
		fmt.Printf("Closed producer channel %d\n", i)
	}
	rb.mu.Unlock()

	if wait {
		fmt.Println("Waiting for consumers to drain queues...")
		for {
			rb.mu.Lock()
			remainingBuffers := len(rb.ringBuffers)
			totalItems := 0
			for _, buf := range rb.ringBuffers {
				totalItems += int(buf.Len())
			}
			rb.mu.Unlock()

			if remainingBuffers == 0 && totalItems == 0 {
				break
			}
			fmt.Printf("Remaining buffers: %d, Total items: %d\n", remainingBuffers, totalItems)
			time.Sleep(100 * time.Millisecond)
		}
	}

	rb.mu.Lock()
	fmt.Println("Closing consumer channels...")
	for i, cc := range rb.consumerChannels {
		close(cc.closed)
		fmt.Printf("Closed consumer channel %d\n", i)
	}

	rb.producerChannels = nil
	rb.consumerChannels = nil
	rb.ringBuffers = nil
	rb.currentBuffer = nil
	rb.mu.Unlock()

	fmt.Println("RollingBuffer closed.")
}

func (pc ProducerChannel) Write(data interface{}) error {
	if pc.state.Load() == int32(ChannelClosed) {
		return fmt.Errorf("channel is closed")
	}
	select {
	case pc.ch <- data:
		return nil
	default:
		return fmt.Errorf("channel is full")
	}
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
	bufferSize := 1024 * 8
	consumerSize := 1024
	numProducers := 10
	numConsumers := 10
	producerWorkers := 10
	consumerWorkers := 10

	rb := NewRollingBuffer(bufferSize, consumerSize)
	now := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var counter int32
	var wg sync.WaitGroup

	fmt.Println("Starting producers and consumers...")

	for i := 0; i < numProducers; i++ {
		pc := rb.NewProducer(producerWorkers)
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					fmt.Printf("Producer %d stopping\n", id)
					return
				default:
					err := pc.Write(1)
					if err != nil {
						if err.Error() == "channel is closed" {
							fmt.Printf("Producer %d channel closed\n", id)
							return
						}
						time.Sleep(time.Millisecond)
					}
				}
			}
		}(i)
	}

	for i := 0; i < numConsumers; i++ {
		cc := rb.NewConsumer(consumerWorkers)
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for {
				select {
				case _, ok := <-cc.ch:
					if !ok {
						fmt.Printf("Consumer %d channel closed\n", id)
						return
					}
					atomic.AddInt32(&counter, 1)
				case <-cc.closed:
					fmt.Printf("Consumer %d received close signal\n", id)
					return
				}
			}
		}(i)
	}

	fmt.Println("Waiting for timeout...")
	<-ctx.Done()
	fmt.Println("Timeout reached. Closing RollingBuffer...")

	rb.Close(true)

	fmt.Println("Waiting for all goroutines to finish...")
	wg.Wait()

	elapsed := time.Since(now)
	messagesPerSecond := float64(counter) / elapsed.Seconds()

	fmt.Printf("Elapsed time: %v\n", elapsed)
	fmt.Printf("Total messages processed: %d\n", counter)
	fmt.Printf("Messages processed per second: %.2f\n", messagesPerSecond)
}

type buffer[T any] struct {
	items           []T
	head, tail, mod int64
}

type RingBuffer[T any] struct {
	len     int64
	content *buffer[T]
	mu      sync.Mutex
}

func New[T any](size int64) *RingBuffer[T] {
	return &RingBuffer[T]{
		content: &buffer[T]{
			items: make([]T, size),
			head:  0,
			tail:  0,
			mod:   size,
		},
		len: 0,
	}
}

func (rb *RingBuffer[T]) Push(item T) {
	rb.mu.Lock()
	rb.content.tail = (rb.content.tail + 1) % rb.content.mod
	if rb.content.tail == rb.content.head {
		size := rb.content.mod * 2
		newBuff := make([]T, size)
		for i := int64(0); i < rb.content.mod; i++ {
			idx := (rb.content.tail + i) % rb.content.mod
			newBuff[i] = rb.content.items[idx]
		}
		content := &buffer[T]{
			items: newBuff,
			head:  0,
			tail:  rb.content.mod,
			mod:   size,
		}
		rb.content = content
	}
	atomic.AddInt64(&rb.len, 1)
	rb.content.items[rb.content.tail] = item
	rb.mu.Unlock()
}

func (rb *RingBuffer[T]) Len() int64 {
	return atomic.LoadInt64(&rb.len)
}

func (rb *RingBuffer[T]) Pop() (T, bool) {
	if rb.Len() == 0 {
		var t T
		return t, false
	}
	rb.mu.Lock()
	rb.content.head = (rb.content.head + 1) % rb.content.mod
	item := rb.content.items[rb.content.head]
	var t T
	rb.content.items[rb.content.head] = t
	atomic.AddInt64(&rb.len, -1)
	rb.mu.Unlock()
	return item, true
}

func (rb *RingBuffer[T]) PopN(n int64) ([]T, bool) {
	if rb.Len() == 0 {
		return nil, false
	}
	rb.mu.Lock()
	content := rb.content

	if n >= rb.len {
		n = rb.len
	}
	atomic.AddInt64(&rb.len, -n)

	items := make([]T, n)
	for i := int64(0); i < n; i++ {
		pos := (content.head + 1 + i) % content.mod
		items[i] = content.items[pos]
		var t T
		content.items[pos] = t
	}
	content.head = (content.head + n) % content.mod

	rb.mu.Unlock()
	return items, true
}

func (rb *RingBuffer[T]) IsFull() bool {
	return rb.Len() == rb.content.mod
}

func (rb *RingBuffer[T]) IsEmpty() bool {
	return rb.Len() == 0
}
