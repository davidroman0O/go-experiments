package main

import (
	"context"
	"fmt"
	"math"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

const (
	defaultBufferSize   = 1024 * 8
	defaultConsumerSize = 1024
)

type ChannelState int32

const (
	ChannelOpen ChannelState = iota
	ChannelClosed
)

type ProducerChannel struct {
	ch            chan interface{}
	state         *atomic.Int32
	workerCount   int
	workerCtxs    []context.Context
	workerCancels []context.CancelFunc
	mu            sync.Mutex
}

type ConsumerChannel struct {
	ch            chan interface{}
	closed        chan struct{}
	workerCount   int
	workerCtxs    []context.Context
	workerCancels []context.CancelFunc
	mu            sync.Mutex
}

type RingBuffer struct {
	buffer []interface{}
	head   int
	tail   int
	count  int
	size   int
	mu     sync.Mutex
}

type AdaptiveWorkerManager struct {
	producerWorkers map[int]*atomic.Int32
	consumerWorkers map[int]*atomic.Int32
	deltas          []float64
	lastAdjustment  time.Time
	mu              sync.Mutex
}

func NewAdaptiveWorkerManager() *AdaptiveWorkerManager {
	return &AdaptiveWorkerManager{
		producerWorkers: make(map[int]*atomic.Int32),
		consumerWorkers: make(map[int]*atomic.Int32),
		deltas:          make([]float64, 0, 10), // Keep last 10 deltas
		lastAdjustment:  time.Now(),
	}
}

func (awm *AdaptiveWorkerManager) AddProducer(idx int, initialWorkers int) {
	awm.mu.Lock()
	defer awm.mu.Unlock()
	awm.producerWorkers[idx] = &atomic.Int32{}
	awm.producerWorkers[idx].Store(int32(initialWorkers))
}

func (awm *AdaptiveWorkerManager) AddConsumer(idx int, initialWorkers int) {
	awm.mu.Lock()
	defer awm.mu.Unlock()
	awm.consumerWorkers[idx] = &atomic.Int32{}
	awm.consumerWorkers[idx].Store(int32(initialWorkers))
}

func (awm *AdaptiveWorkerManager) AdjustWorkers(rb *RollingBuffer, currentDelta int32) {
	awm.mu.Lock()
	awm.deltas = append(awm.deltas, float64(currentDelta))
	if len(awm.deltas) > 10 {
		awm.deltas = awm.deltas[1:]
	}

	if len(awm.deltas) < 2 {
		awm.mu.Unlock()
		return // Not enough data to make a decision
	}

	// Calculate rate of change
	rateOfChange := (awm.deltas[len(awm.deltas)-1] - awm.deltas[0]) / float64(len(awm.deltas))

	// Collect current worker counts
	producerCounts := make(map[int]int32)
	consumerCounts := make(map[int]int32)
	for idx, workers := range awm.producerWorkers {
		producerCounts[idx] = workers.Load()
	}
	for idx, workers := range awm.consumerWorkers {
		consumerCounts[idx] = workers.Load()
	}
	awm.mu.Unlock()

	// Calculate adjustment based on the current delta and rate of change
	adjustment := int(math.Ceil(rateOfChange / 1000))

	if adjustment == 0 {
		if rateOfChange > 0 {
			adjustment = 1
		} else if rateOfChange < 0 {
			adjustment = -1
		}
	}

	if currentDelta > 0 {
		// Need more consumers, fewer producers
		for idx := range consumerCounts {
			for i := 0; i < adjustment; i++ {
				rb.addConsumerWorker(idx)
				awm.consumerWorkers[idx].Add(1)
			}
		}
		for idx := range producerCounts {
			for i := 0; i < adjustment && awm.GetWorkerCount(true, idx) > 1; i++ {
				rb.removeProducerWorker(idx)
				awm.producerWorkers[idx].Add(-1)
			}
		}
	} else if currentDelta < 0 {
		// Need more producers, fewer consumers
		for idx := range producerCounts {
			for i := 0; i < -adjustment; i++ {
				rb.addProducerWorker(idx)
				awm.producerWorkers[idx].Add(1)
			}
		}
		for idx := range consumerCounts {
			for i := 0; i < -adjustment && awm.GetWorkerCount(false, idx) > 1; i++ {
				rb.removeConsumerWorker(idx)
				awm.consumerWorkers[idx].Add(-1)
			}
		}
	}

	// Always ensure at least one producer and one consumer
	if awm.GetTotalWorkers(true) == 0 {
		for idx := range producerCounts {
			rb.addProducerWorker(idx)
			awm.producerWorkers[idx].Add(1)
			break
		}
	}
	if awm.GetTotalWorkers(false) == 0 {
		for idx := range consumerCounts {
			rb.addConsumerWorker(idx)
			awm.consumerWorkers[idx].Add(1)
			break
		}
	}

	fmt.Printf("Current Delta: %d, Rate of Change: %.2f, Adjustment: %d, Producers: %d, Consumers: %d\n",
		currentDelta, rateOfChange, adjustment, awm.GetTotalWorkers(true), awm.GetTotalWorkers(false))
}

func (awm *AdaptiveWorkerManager) GetWorkerCount(isProducer bool, idx int) int {
	awm.mu.Lock()
	defer awm.mu.Unlock()
	if isProducer {
		return int(awm.producerWorkers[idx].Load())
	}
	return int(awm.consumerWorkers[idx].Load())
}

func (awm *AdaptiveWorkerManager) GetTotalWorkers(isProducer bool) int {
	awm.mu.Lock()
	defer awm.mu.Unlock()
	total := 0
	if isProducer {
		for _, workers := range awm.producerWorkers {
			total += int(workers.Load())
		}
	} else {
		for _, workers := range awm.consumerWorkers {
			total += int(workers.Load())
		}
	}
	return total
}

type RollingBuffer struct {
	producerChannels []*ProducerChannel
	consumerChannels []*ConsumerChannel
	ringBuffers      []*RingBuffer
	currentBuffer    *RingBuffer
	bufferSize       int
	consumerSize     int
	mu               sync.Mutex
	workerManager    *AdaptiveWorkerManager
	ctx              context.Context
	cancel           context.CancelFunc
	producedCount    int32
	consumedCount    int32
}

func NewRollingBuffer(bufferSize, consumerSize int) *RollingBuffer {
	ctx, cancel := context.WithCancel(context.Background())
	rb := &RollingBuffer{
		bufferSize:    bufferSize,
		consumerSize:  consumerSize,
		workerManager: NewAdaptiveWorkerManager(),
		ctx:           ctx,
		cancel:        cancel,
	}
	rb.currentBuffer = NewRingBuffer(bufferSize)
	rb.ringBuffers = append(rb.ringBuffers, rb.currentBuffer)

	go rb.manageWorkers()

	return rb
}

func NewRingBuffer(size int) *RingBuffer {
	return &RingBuffer{
		buffer: make([]interface{}, size),
		size:   size,
	}
}

func (rb *RollingBuffer) NewProducer(idx, initialWorkers int) *ProducerChannel {
	ch := make(chan interface{}, rb.bufferSize)
	pc := &ProducerChannel{
		ch:          ch,
		state:       &atomic.Int32{},
		workerCount: initialWorkers,
	}
	rb.mu.Lock()
	rb.producerChannels = append(rb.producerChannels, pc)
	rb.mu.Unlock()
	rb.workerManager.AddProducer(idx, initialWorkers)
	for i := 0; i < initialWorkers; i++ {
		rb.addProducerWorker(idx)
	}
	return pc
}

func (rb *RollingBuffer) NewConsumer(idx, initialWorkers int) *ConsumerChannel {
	ch := make(chan interface{}, rb.consumerSize)
	closed := make(chan struct{})
	cc := &ConsumerChannel{ch: ch, closed: closed, workerCount: initialWorkers}
	rb.mu.Lock()
	rb.consumerChannels = append(rb.consumerChannels, cc)
	rb.mu.Unlock()
	rb.workerManager.AddConsumer(idx, initialWorkers)
	for i := 0; i < initialWorkers; i++ {
		rb.addConsumerWorker(idx)
	}
	return cc
}

func (rb *RollingBuffer) producerWorker(ctx context.Context, ch <-chan interface{}) {
	for {
		select {
		case <-ctx.Done():
			return
		case data := <-ch:
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
			atomic.AddInt32(&rb.producedCount, 1)
		}
	}
}

func (rb *RollingBuffer) consumerWorker(ctx context.Context, ch chan<- interface{}) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			data, ok := rb.read()
			if ok {
				select {
				case ch <- data:
					atomic.AddInt32(&rb.consumedCount, 1)
				case <-ctx.Done():
					// Put the data back if we're shutting down
					rb.mu.Lock()
					rb.currentBuffer.Write(data)
					rb.mu.Unlock()
					return
				}
			} else {
				time.Sleep(time.Millisecond)
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

func (rb *RollingBuffer) manageWorkers() {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			producedCount := atomic.LoadInt32(&rb.producedCount)
			consumedCount := atomic.LoadInt32(&rb.consumedCount)
			delta := producedCount - consumedCount
			rb.workerManager.AdjustWorkers(rb, delta)
		case <-rb.ctx.Done():
			return
		}
	}
}

func (rb *RollingBuffer) addProducerWorker(idx int) {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	pc := rb.producerChannels[idx]
	pc.mu.Lock()
	defer pc.mu.Unlock()
	ctx, cancel := context.WithCancel(context.Background())
	pc.workerCtxs = append(pc.workerCtxs, ctx)
	pc.workerCancels = append(pc.workerCancels, cancel)
	go rb.producerWorker(ctx, pc.ch)
	pc.workerCount++
}

func (rb *RollingBuffer) removeProducerWorker(idx int) {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	pc := rb.producerChannels[idx]
	pc.mu.Lock()
	defer pc.mu.Unlock()
	if pc.workerCount > 1 {
		lastIndex := len(pc.workerCancels) - 1
		pc.workerCancels[lastIndex]()
		pc.workerCtxs = pc.workerCtxs[:lastIndex]
		pc.workerCancels = pc.workerCancels[:lastIndex]
		pc.workerCount--
	}
}

func (rb *RollingBuffer) addConsumerWorker(idx int) {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	cc := rb.consumerChannels[idx]
	cc.mu.Lock()
	defer cc.mu.Unlock()
	ctx, cancel := context.WithCancel(context.Background())
	cc.workerCtxs = append(cc.workerCtxs, ctx)
	cc.workerCancels = append(cc.workerCancels, cancel)
	go rb.consumerWorker(ctx, cc.ch)
	cc.workerCount++
}

func (rb *RollingBuffer) removeConsumerWorker(idx int) {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	cc := rb.consumerChannels[idx]
	cc.mu.Lock()
	defer cc.mu.Unlock()
	if cc.workerCount > 1 {
		lastIndex := len(cc.workerCancels) - 1
		cc.workerCancels[lastIndex]()
		cc.workerCtxs = cc.workerCtxs[:lastIndex]
		cc.workerCancels = cc.workerCancels[:lastIndex]
		cc.workerCount--
	}
}

func (rb *RollingBuffer) Close(wait bool) {
	fmt.Println("Starting Close function...")
	rb.cancel() // Stop the worker management goroutine

	rb.mu.Lock()
	fmt.Println("Closing producer channels...")
	for i := 0; i < len(rb.producerChannels); i++ {
		pc := rb.producerChannels[i]
		pc.state.Store(int32(ChannelClosed))
		close(pc.ch)
		pc.mu.Lock()
		for _, cancel := range pc.workerCancels {
			cancel()
		}
		pc.mu.Unlock()
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
	for i := 0; i < len(rb.consumerChannels); i++ {
		cc := rb.consumerChannels[i]
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

func (pc *ProducerChannel) Write(data interface{}) error {
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
	consumerSize := 1024 * 8
	numProducers := 1
	numConsumers := 1
	producerWorkers := 2
	consumerWorkers := 2

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
		pc := rb.NewProducer(i, producerWorkers)
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					fmt.Printf("Producer %d stopping\n", idx)
					return
				default:
					err := pc.Write(1)
					if err != nil {
						if err.Error() == "channel is closed" {
							fmt.Printf("Producer %d channel closed\n", idx)
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
		cc := rb.NewConsumer(i, consumerWorkers)
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			for {
				select {
				case _, ok := <-cc.ch:
					if !ok {
						fmt.Printf("Consumer %d channel closed\n", idx)
						return
					}
					atomic.AddInt32(&counterConsumer, 1)
				case <-cc.closed:
					fmt.Printf("Consumer %d received close signal\n", idx)
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
