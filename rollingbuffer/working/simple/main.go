package main

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// Fully working without data-race
// By far the stablest version of the code

const defaultThroughput = 512

// borrowed from anthonygg
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

type RingBuffer struct {
	buffer []interface{}
	head   int
	tail   int
	count  int
	size   int
	mu     sync.Mutex
}

type RollingBuffer struct {
	producerChannels   []ProducerChannel
	consumerChannels   []ConsumerChannel
	ringBuffers        []*RingBuffer
	currentBuffer      *RingBuffer
	bufferSize         int
	consumerSize       int
	mu                 sync.Mutex
	schedulerProducers Scheduler
	schedulerConsumers Scheduler
}

func NewRollingBuffer(bufferSize, consumerSize int) *RollingBuffer {
	rb := &RollingBuffer{
		bufferSize:         bufferSize,
		consumerSize:       consumerSize,
		schedulerProducers: NewScheduler(defaultThroughput),
		schedulerConsumers: NewScheduler(defaultThroughput),
	}
	rb.currentBuffer = NewRingBuffer(bufferSize)
	rb.ringBuffers = append(rb.ringBuffers, rb.currentBuffer)
	return rb
}

func NewRingBuffer(size int) *RingBuffer {
	return &RingBuffer{
		buffer: make([]interface{}, size),
		size:   size,
	}
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
		rb.schedulerProducers.Schedule(func() { rb.producerWorker(ch) })
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
		rb.schedulerConsumers.Schedule(func() { rb.consumerWorker(ch, closed) })
	}
	return cc
}

func (rb *RollingBuffer) producerWorker(ch <-chan interface{}) {
	i, t := 0, rb.schedulerProducers.Throughput()
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
			rb.currentBuffer = NewRingBuffer(rb.bufferSize)
			rb.ringBuffers = append(rb.ringBuffers, rb.currentBuffer)
		}
		rb.currentBuffer.Write(data)
		rb.mu.Unlock()
	}
}

func (rb *RollingBuffer) consumerWorker(ch chan<- interface{}, closed <-chan struct{}) {
	i, t := 0, rb.schedulerConsumers.Throughput()
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
			data := buffer.Read()
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
				totalItems += buf.count
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

func (rb *RingBuffer) Write(data interface{}) {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	rb.buffer[rb.tail] = data
	rb.tail = (rb.tail + 1) % rb.size
	rb.count++
}

func (rb *RingBuffer) Read() interface{} {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	data := rb.buffer[rb.head]
	rb.buffer[rb.head] = nil // Allow GC to collect the data
	rb.head = (rb.head + 1) % rb.size
	rb.count--
	return data
}

func (rb *RingBuffer) IsFull() bool {
	return rb.count == rb.size
}

func (rb *RingBuffer) IsEmpty() bool {
	return rb.count == 0
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
	consumerWorkers := 20

	rb := NewRollingBuffer(bufferSize, consumerSize)
	now := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var counterProducer int32
	var counterConsumer int32
	var delta int32
	var wg sync.WaitGroup

	ticker := time.NewTicker(1 * time.Second)
	go func() {
		for {
			select {
			case <-ticker.C:
				fmt.Printf("Messages produced: %d\n", atomic.LoadInt32(&counterProducer))
				fmt.Printf("Messages consumed: %d\n", atomic.LoadInt32(&counterConsumer))
				delta = atomic.LoadInt32(&counterProducer) - atomic.LoadInt32(&counterConsumer)
				fmt.Printf("Delta: %d\n", delta)
			case <-ctx.Done():
				return
			}
		}
	}()

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
					atomic.AddInt32(&counterProducer, 1)
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
					atomic.AddInt32(&counterConsumer, 1)
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
	messagesPerSecond := float64(counterConsumer) / elapsed.Seconds()
	messagesPerSecondProducer := float64(counterProducer) / elapsed.Seconds()

	fmt.Printf("Elapsed time: %v\n", elapsed)
	fmt.Printf("Total messages produced: %d\n", counterProducer)
	fmt.Printf("Messages produced per second: %.2f\n", messagesPerSecondProducer)
	fmt.Printf("Total messages consumed: %d\n", counterConsumer)
	fmt.Printf("Messages consumed per second: %.2f\n", messagesPerSecond)
}
