package main

import (
	"context"
	"fmt"
	"math/rand"
	"reflect"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

const (
	cacheLine = 64
)

type padding [cacheLine]byte

// Event represents a single event in the ring buffer
type Event struct {
	value unsafe.Pointer
	_     padding
}

// EventPool manages a pool of pre-allocated Event objects
type EventPool struct {
	pool sync.Pool
}

// NewEventPool creates a new EventPool
func NewEventPool(typ reflect.Type) *EventPool {
	return &EventPool{
		pool: sync.Pool{
			New: func() interface{} {
				return &Event{value: unsafe.Pointer(reflect.New(typ).Pointer())}
			},
		},
	}
}

// LockFreeRingBuffer implements a lock-free ring buffer
type LockFreeRingBuffer struct {
	buffer    []*Event
	mask      uint64
	producer  atomic.Uint64
	consumer  atomic.Uint64
	eventPool *EventPool
}

// NewLockFreeRingBuffer creates a new LockFreeRingBuffer
func NewLockFreeRingBuffer(size uint64, eventPool *EventPool) *LockFreeRingBuffer {
	rb := &LockFreeRingBuffer{
		buffer:    make([]*Event, size),
		mask:      size - 1,
		eventPool: eventPool,
	}
	for i := range rb.buffer {
		rb.buffer[i] = rb.eventPool.pool.Get().(*Event)
	}
	return rb
}

// MultiBufferDisruptor manages multiple ring buffers
type MultiBufferDisruptor struct {
	singleBuffers []*LockFreeRingBuffer
	batchBuffers  []*LockFreeRingBuffer
	currentSingle atomic.Uint64
	currentBatch  atomic.Uint64
}

// AllocationProgress represents the progress of pre-allocation
type AllocationProgress struct {
	Total   int64
	Current atomic.Int64
}

func (p *AllocationProgress) Increment() {
	p.Current.Add(1)
}

func (p *AllocationProgress) GetProgress() float64 {
	return float64(p.Current.Load()) / float64(p.Total)
}

// Disruptor is the main structure managing the entire system
type Disruptor struct {
	ctx                context.Context
	cancel             context.CancelFunc
	multiBuffer        *MultiBufferDisruptor
	handler            func([]interface{})
	consumers          []*Consumer
	producers          []*Producer
	batcher            *AdaptiveBatcher
	backpressure       *BackpressureController
	metrics            *Metrics
	allocationProgress *AllocationProgress
	wg                 sync.WaitGroup
	mode               Mode
}

type Mode int

const (
	DispatchMode Mode = iota
	BroadcastMode
)

// DisruptorOption allows for customization of the Disruptor
type DisruptorOption func(*Disruptor)

// NewDisruptor creates a new Disruptor with the given options
func NewDisruptor(
	ctx context.Context,
	singleBufferCount, singleBufferSize,
	batchBufferCount, batchBufferSize int,
	typ reflect.Type,
	handler func([]interface{}),
	options ...DisruptorOption,
) *Disruptor {
	eventPool := NewEventPool(typ)
	ctxWithCancel, cancel := context.WithCancel(ctx)
	d := &Disruptor{
		ctx:    ctxWithCancel,
		cancel: cancel,
		multiBuffer: &MultiBufferDisruptor{
			singleBuffers: make([]*LockFreeRingBuffer, singleBufferCount),
			batchBuffers:  make([]*LockFreeRingBuffer, batchBufferCount),
		},
		handler:      handler,
		batcher:      NewAdaptiveBatcher(1, 1000),
		backpressure: NewBackpressureController(0.8),
		metrics:      NewMetrics(),
		allocationProgress: &AllocationProgress{
			Total: int64(singleBufferCount*singleBufferSize + batchBufferCount*batchBufferSize),
		},
		mode: DispatchMode,
	}

	for i := range d.multiBuffer.singleBuffers {
		d.multiBuffer.singleBuffers[i] = NewLockFreeRingBuffer(uint64(singleBufferSize), eventPool)
		for j := 0; j < singleBufferSize; j++ {
			d.allocationProgress.Increment()
		}
	}
	for i := range d.multiBuffer.batchBuffers {
		d.multiBuffer.batchBuffers[i] = NewLockFreeRingBuffer(uint64(batchBufferSize), eventPool)
		for j := 0; j < batchBufferSize; j++ {
			d.allocationProgress.Increment()
		}
	}

	for _, option := range options {
		option(d)
	}

	d.createProducersAndConsumers()

	return d
}

func (d *Disruptor) createProducersAndConsumers() {
	// Create producers
	d.producers = make([]*Producer, runtime.NumCPU())
	for i := range d.producers {
		d.producers[i] = &Producer{disruptor: d}
		d.wg.Add(1)
	}

	// Create consumers
	d.consumers = make([]*Consumer, runtime.NumCPU())
	for i := range d.consumers {
		d.consumers[i] = &Consumer{disruptor: d, id: uint32(i)}
		d.wg.Add(1)
	}
}

// Publish publishes a single value to the disruptor
func (d *Disruptor) Publish(value interface{}) bool {
	select {
	case <-d.ctx.Done():
		return false
	default:
		rb := d.multiBuffer.singleBuffers[d.multiBuffer.currentSingle.Add(1)%uint64(len(d.multiBuffer.singleBuffers))]
		return rb.TryPublish(value)
	}
}

// PublishBatch publishes a batch of values to the disruptor
func (d *Disruptor) PublishBatch(batch []interface{}) bool {
	select {
	case <-d.ctx.Done():
		return false
	default:
		rb := d.multiBuffer.batchBuffers[d.multiBuffer.currentBatch.Add(1)%uint64(len(d.multiBuffer.batchBuffers))]
		return rb.TryPublishBatch(batch)
	}
}

// Start starts the disruptor
func (d *Disruptor) Start() {
	for _, p := range d.producers {
		go p.run()
	}
	for _, c := range d.consumers {
		go c.run()
	}
}

// Stop stops the disruptor
func (d *Disruptor) Stop() {
	d.cancel()
	d.Wait()
}

// Wait waits for all producers and consumers to finish
func (d *Disruptor) Wait() {
	d.wg.Wait()
}

// GetAllocationProgress returns the current allocation progress
func (d *Disruptor) GetAllocationProgress() float64 {
	return d.allocationProgress.GetProgress()
}

// WithProducerCount sets the number of producers
func WithProducerCount(count int) DisruptorOption {
	return func(d *Disruptor) {
		d.producers = make([]*Producer, count)
		for i := range d.producers {
			d.producers[i] = &Producer{disruptor: d}
			d.wg.Add(1)
		}
	}
}

// WithConsumerCount sets the number of consumers
func WithConsumerCount(count int) DisruptorOption {
	return func(d *Disruptor) {
		d.consumers = make([]*Consumer, count)
		for i := range d.consumers {
			d.consumers[i] = &Consumer{disruptor: d, id: uint32(i)}
			d.wg.Add(1)
		}
	}
}

// WithDispatchMode sets the disruptor to dispatch mode
func WithDispatchMode() DisruptorOption {
	return func(d *Disruptor) {
		d.mode = DispatchMode
	}
}

// WithBroadcastMode sets the disruptor to broadcast mode
func WithBroadcastMode() DisruptorOption {
	return func(d *Disruptor) {
		d.mode = BroadcastMode
	}
}

// Producer represents a producer in the system
type Producer struct {
	disruptor *Disruptor
}

func (p *Producer) run() {
	defer p.disruptor.wg.Done()
	for {
		select {
		case <-p.disruptor.ctx.Done():
			return
		default:
			if p.disruptor.backpressure.ShouldThrottle() {
				time.Sleep(time.Millisecond)
				continue
			}
			value := rand.Int63()
			if p.disruptor.Publish(value) {
				p.disruptor.metrics.IncrementProduced(1)
			}
		}
	}
}

// Consumer represents a consumer in the system
type Consumer struct {
	disruptor *Disruptor
	id        uint32
	sequence  atomic.Uint64
}

func (c *Consumer) consume() int {
	batch := make([]interface{}, c.disruptor.batcher.GetBatchSize())
	count := 0
	for i := range batch {
		if c.disruptor.mode == DispatchMode {
			if (c.sequence.Load()+uint64(i))%uint64(len(c.disruptor.consumers)) != uint64(c.id) {
				continue
			}
		}

		// Try to consume from all single buffers
		consumed := false
		for _, rb := range c.disruptor.multiBuffer.singleBuffers {
			value, ok := rb.TryConsume()
			if ok {
				batch[count] = value
				count++
				consumed = true
				break
			}
		}

		if !consumed {
			// If nothing consumed from single buffers, try batch buffers
			for _, rb := range c.disruptor.multiBuffer.batchBuffers {
				value, ok := rb.TryConsume()
				if ok {
					batch[count] = value
					count++
					consumed = true
					break
				}
			}
		}

		if !consumed {
			break
		}
	}

	if count > 0 {
		c.disruptor.handler(batch[:count])
		c.sequence.Add(uint64(count))
	}
	return count
}

func (c *Consumer) run() {
	defer c.disruptor.wg.Done()
	for {
		select {
		case <-c.disruptor.ctx.Done():
			return
		default:
			consumed := c.consume()
			if consumed > 0 {
				c.disruptor.metrics.IncrementConsumed(uint64(consumed))
			} else {
				runtime.Gosched()
			}
		}
	}
}

// AdaptiveBatcher manages batch sizes for producers and consumers
type AdaptiveBatcher struct {
	batchSize atomic.Uint32
	min       uint32
	max       uint32
}

func NewAdaptiveBatcher(min, max uint32) *AdaptiveBatcher {
	ab := &AdaptiveBatcher{
		min: min,
		max: max,
	}
	ab.batchSize.Store(min)
	return ab
}

func (ab *AdaptiveBatcher) GetBatchSize() uint32 {
	return ab.batchSize.Load()
}

func (ab *AdaptiveBatcher) IncreaseBatchSize() {
	for {
		current := ab.batchSize.Load()
		if current >= ab.max {
			return
		}
		next := current * 2
		if next > ab.max {
			next = ab.max
		}
		if ab.batchSize.CompareAndSwap(current, next) {
			return
		}
	}
}

func (ab *AdaptiveBatcher) DecreaseBatchSize() {
	for {
		current := ab.batchSize.Load()
		if current <= ab.min {
			return
		}
		next := current / 2
		if next < ab.min {
			next = ab.min
		}
		if ab.batchSize.CompareAndSwap(current, next) {
			return
		}
	}
}

// BackpressureController manages producer throttling
type BackpressureController struct {
	threshold float64
}

func NewBackpressureController(threshold float64) *BackpressureController {
	return &BackpressureController{threshold: threshold}
}

func (bc *BackpressureController) ShouldThrottle() bool {
	// Implement backpressure logic here
	return false
}

// Metrics gathers performance data
type Metrics struct {
	producerThroughput atomic.Uint64
	consumerThroughput atomic.Uint64
}

func NewMetrics() *Metrics {
	return &Metrics{}
}

func (m *Metrics) IncrementProduced(count uint64) {
	m.producerThroughput.Add(count)
}

func (m *Metrics) IncrementConsumed(count uint64) {
	m.consumerThroughput.Add(count)
}

func (rb *LockFreeRingBuffer) TryPublish(value interface{}) bool {
	for {
		head := rb.producer.Load()
		tail := rb.consumer.Load()

		if head-tail >= rb.mask {
			return false // Buffer full
		}

		if rb.producer.CompareAndSwap(head, head+1) {
			*(*interface{})(rb.buffer[head&rb.mask].value) = value
			return true
		}
	}
}

func (rb *LockFreeRingBuffer) TryPublishBatch(batch []interface{}) bool {
	n := uint64(len(batch))
	for {
		head := rb.producer.Load()
		tail := rb.consumer.Load()

		if head-tail+n > rb.mask {
			return false // Not enough space
		}

		if rb.producer.CompareAndSwap(head, head+n) {
			for i, value := range batch {
				*(*interface{})(rb.buffer[(head+uint64(i))&rb.mask].value) = value
			}
			return true
		}
	}
}

func (rb *LockFreeRingBuffer) TryConsume() (interface{}, bool) {
	for {
		tail := rb.consumer.Load()
		head := rb.producer.Load()

		if tail == head {
			return nil, false // Buffer empty
		}

		if rb.consumer.CompareAndSwap(tail, tail+1) {
			return *(*interface{})(rb.buffer[tail&rb.mask].value), true
		}
	}
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	bufferSizes := []int{1024 * 1024}
	modes := []Mode{DispatchMode, BroadcastMode}
	workerCounts := []int{1, 2, 4, 8}
	testModes := []string{"SinglePublish", "BatchPublish", "MixedPublish"}

	for _, bufferSize := range bufferSizes {
		for _, mode := range modes {
			for _, workerCount := range workerCounts {
				for _, testMode := range testModes {
					runTest(bufferSize, mode, workerCount, testMode)
				}
			}
		}
	}
}

func runTest(bufferSize int, mode Mode, workerCount int, testMode string) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var modeOption DisruptorOption
	if mode == DispatchMode {
		modeOption = WithDispatchMode()
	} else {
		modeOption = WithBroadcastMode()
	}
	d := NewDisruptor(
		ctx,
		workerCount, bufferSize,
		workerCount, bufferSize,
		reflect.TypeOf(int64(0)),
		func(data []interface{}) {
			// Process data
			for range data {
				// Simulate some work
				time.Sleep(time.Nanosecond)
			}
		},
		WithProducerCount(workerCount),
		WithConsumerCount(workerCount),
		modeOption,
	)

	fmt.Printf("Allocation progress: %.2f%%\n", d.GetAllocationProgress()*100)

	d.Start()

	startTime := time.Now()

	switch testMode {
	case "SinglePublish":
		for i := 0; i < workerCount; i++ {
			go func() {
				for {
					if d.Publish(rand.Int63()) {
						d.metrics.IncrementProduced(1)
					}
					if ctx.Err() != nil {
						return
					}
				}
			}()
		}
	case "BatchPublish":
		for i := 0; i < workerCount; i++ {
			go func() {
				batch := make([]interface{}, 100)
				for {
					for i := range batch {
						batch[i] = rand.Int63()
					}
					if d.PublishBatch(batch) {
						d.metrics.IncrementProduced(uint64(len(batch)))
					}
					if ctx.Err() != nil {
						return
					}
				}
			}()
		}
	case "MixedPublish":
		for i := 0; i < workerCount; i++ {
			go func() {
				batch := make([]interface{}, 10)
				for {
					if rand.Float32() < 0.5 {
						if d.Publish(rand.Int63()) {
							d.metrics.IncrementProduced(1)
						}
					} else {
						for i := range batch {
							batch[i] = rand.Int63()
						}
						if d.PublishBatch(batch) {
							d.metrics.IncrementProduced(uint64(len(batch)))
						}
					}
					if ctx.Err() != nil {
						return
					}
				}
			}()
		}
	}

	<-ctx.Done()
	d.Stop()
	d.Wait()

	duration := time.Since(startTime)
	totalProduced := d.metrics.producerThroughput.Load()
	totalConsumed := d.metrics.consumerThroughput.Load()

	fmt.Printf("Test: BufferSize=%d, Mode=%v, Workers=%d, TestMode=%s\n", bufferSize, mode, workerCount, testMode)
	fmt.Printf("Duration: %v\n", duration)
	fmt.Printf("Total produced: %d\n", totalProduced)
	fmt.Printf("Total consumed: %d\n", totalConsumed)
	fmt.Printf("Producer throughput: %.2f million ops/sec\n", float64(totalProduced)/duration.Seconds()/1e6)
	fmt.Printf("Consumer throughput: %.2f million ops/sec\n\n", float64(totalConsumed)/duration.Seconds()/1e6)
}
