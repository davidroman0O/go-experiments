package main

import (
	"context"
	"fmt"
	"reflect"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

const (
	cacheLine         = 64
	producerBatchSize = 8192
	consumerBatchSize = 8192
)

type padding [cacheLine]byte

type RingBuffer struct {
	buffer               unsafe.Pointer
	bufferMask           int64
	_padding0            padding
	cursor               *Sequence
	_padding1            padding
	gatingSequences      []*Sequence
	_padding2            padding
	elementSize          uintptr
	_padding3            padding
	consumedSequences    []int64
	_padding4            padding
	multiConsume         bool
	_padding5            padding
	lastProducedSequence int64
	_padding6            padding
	lastConsumedSequence int64
	_padding7            padding
	elementType          reflect.Type
	_padding8            padding
	producerStalls       uint64
	_padding9            padding
	consumerStalls       uint64
	_padding10           padding
	maxBatchSize         uint64
	_padding11           padding
	totalBatches         uint64
	_padding12           padding
	ctx                  context.Context
	cancel               context.CancelFunc
	wg                   sync.WaitGroup
}

type Sequence struct {
	value    uint64
	_padding padding
}

//go:nosplit
func NewSequence(initial int64) *Sequence {
	return &Sequence{value: uint64(initial)}
}

//go:nosplit
func (s *Sequence) Get() int64 {
	return int64(atomic.LoadUint64(&s.value))
}

//go:nosplit
func (s *Sequence) Set(value int64) {
	atomic.StoreUint64(&s.value, uint64(value))
}

//go:nosplit
func (s *Sequence) IncrementAndGet() int64 {
	return int64(atomic.AddUint64(&s.value, 1)) - 1
}

func NewRingBuffer(bufferSize int64, elementType reflect.Type, consumerCount int, multiConsume bool) *RingBuffer {
	if bufferSize < 1 || ((bufferSize & (bufferSize - 1)) != 0) {
		panic("bufferSize must be a positive power of 2")
	}
	ctx, cancel := context.WithCancel(context.Background())
	rb := &RingBuffer{
		bufferMask:           bufferSize - 1,
		cursor:               NewSequence(-1),
		elementSize:          uintptr(elementType.Size()),
		multiConsume:         multiConsume,
		lastProducedSequence: -1,
		lastConsumedSequence: -1,
		elementType:          elementType,
		ctx:                  ctx,
		cancel:               cancel,
	}

	buffer := make([]interface{}, bufferSize)
	for i := range buffer {
		buffer[i] = reflect.New(elementType).Elem().Interface()
	}
	rb.buffer = unsafe.Pointer(&buffer[0])

	rb.consumedSequences = make([]int64, bufferSize)
	for i := range rb.consumedSequences {
		rb.consumedSequences[i] = -1
	}

	return rb
}

func (rb *RingBuffer) ClaimNextSequences(n int64) (int64, int64) {
	start := atomic.AddInt64(&rb.lastProducedSequence, n) - n
	return start, start + n - 1
}

func (rb *RingBuffer) MarkConsumed(sequence int64) {
	for {
		last := atomic.LoadInt64(&rb.lastConsumedSequence)
		if sequence <= last {
			return
		}
		if atomic.CompareAndSwapInt64(&rb.lastConsumedSequence, last, sequence) {
			return
		}
	}
}

func (rb *RingBuffer) Wait() {
	rb.wg.Wait()
}

func (rb *RingBuffer) Shutdown() {
	rb.cancel()
	rb.Wait()
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

//go:nosplit
func (p *Producer) Publish(value interface{}) {
	select {
	case <-p.rb.ctx.Done():
		return
	default:
		sequence := p.rb.cursor.IncrementAndGet()
		index := sequence & p.rb.bufferMask
		dst := (*interface{})(unsafe.Pointer(uintptr(p.rb.buffer) + uintptr(index)*p.rb.elementSize))
		*dst = value
		atomic.StoreInt64(&p.rb.consumedSequences[index], -1)
		p.rb.cursor.Set(sequence)
		p.waitStrategy.SignalAllWhenBlocking()
		atomic.StoreInt64(&p.rb.lastProducedSequence, sequence)
		runtime.KeepAlive(p.rb.buffer)
	}
}

//go:nosplit
func (p *Producer) PublishBatch(values []interface{}) {
	n := int64(len(values))
	start, end := p.rb.ClaimNextSequences(n)
	sequence := start
	highSequence := end

	for i := int64(0); i < n; i++ {
		index := (sequence + i) & p.rb.bufferMask
		dst := (*interface{})(unsafe.Pointer(uintptr(p.rb.buffer) + uintptr(index)*p.rb.elementSize))
		*dst = values[i]
		atomic.StoreInt64(&p.rb.consumedSequences[index], -1)
	}

	if !p.waitStrategy.TryWaitFor(highSequence, p.rb.cursor) {
		atomic.AddUint64(&p.rb.producerStalls, 1)
	}

	p.rb.cursor.Set(highSequence)
	p.waitStrategy.SignalAllWhenBlocking()
	runtime.KeepAlive(p.rb.buffer)
}

type WaitStrategy interface {
	WaitFor(sequence int64, cursor *Sequence, dependents ...*Sequence) int64
	TryWaitFor(sequence int64, cursor *Sequence) bool
	SignalAllWhenBlocking()
}

type Consumer struct {
	rb           *RingBuffer
	sequence     *Sequence
	handler      func([]interface{})
	waitStrategy WaitStrategy
	_padding0    padding
}

func NewConsumer(rb *RingBuffer, handler func([]interface{}), waitStrategy WaitStrategy) *Consumer {
	return &Consumer{
		rb:           rb,
		sequence:     NewSequence(-1),
		handler:      handler,
		waitStrategy: waitStrategy,
	}
}

// func (c *Consumer) Start() {
// 	c.rb.wg.Add(1)
// 	go func() {
// 		defer c.rb.wg.Done()
// 		c.run(len(c.rb.gatingSequences))
// 		c.rb.gatingSequences = append(c.rb.gatingSequences, c.sequence)
// 	}()
// }

// func (c *Consumer) run(consumerIndex int) {
// 	nextSequence := c.sequence.Get() + 1
// 	eventsBatch := make([]interface{}, consumerBatchSize)

// 	for {
// 		select {
// 		case <-c.rb.ctx.Done():
// 			return
// 		default:
// 			availableSequence := c.waitStrategy.WaitFor(nextSequence, c.rb.cursor, c.sequence)

// 			for nextSequence <= availableSequence {
// 				batchEnd := nextSequence + consumerBatchSize
// 				if batchEnd > availableSequence+1 {
// 					batchEnd = availableSequence + 1
// 				}

// 				batchSize := int64(0)
// 				for i := nextSequence; i < batchEnd; i++ {
// 					index := i & c.rb.bufferMask
// 					if c.rb.multiConsume || atomic.CompareAndSwapInt64(&c.rb.consumedSequences[index], -1, int64(consumerIndex)) {
// 						src := (*interface{})(unsafe.Pointer(uintptr(c.rb.buffer) + uintptr(index)*c.rb.elementSize))
// 						eventsBatch[batchSize] = *src
// 						batchSize++
// 						c.rb.MarkConsumed(i)
// 					}
// 				}

// 				if batchSize > 0 {
// 					atomic.AddUint64(&c.rb.totalBatches, 1)
// 					if uint64(batchSize) > atomic.LoadUint64(&c.rb.maxBatchSize) {
// 						atomic.StoreUint64(&c.rb.maxBatchSize, uint64(batchSize))
// 					}
// 					c.handler(eventsBatch[:batchSize])
// 				} else {
// 					atomic.AddUint64(&c.rb.consumerStalls, 1)
// 				}
// 				nextSequence = batchEnd
// 			}

//				c.sequence.Set(availableSequence)
//			}
//		}
//		runtime.KeepAlive(c.rb.buffer)
//	}
func (c *Consumer) Start() {
	c.rb.wg.Add(1)
	go func() {
		defer c.rb.wg.Done()
		c.run(len(c.rb.gatingSequences))
	}()
}

// func (c *Consumer) run(consumerIndex int) {
// 	nextSequence := c.sequence.Get() + 1
// 	eventsBatch := make([]interface{}, consumerBatchSize)

// 	for {
// 		select {
// 		case <-c.rb.ctx.Done():
// 			return
// 		default:
// 			availableSequence := c.waitStrategy.WaitFor(nextSequence, c.rb.cursor, c.sequence)

// 			for nextSequence <= availableSequence {
// 				batchEnd := nextSequence + consumerBatchSize
// 				if batchEnd > availableSequence+1 {
// 					batchEnd = availableSequence + 1
// 				}

// 				batchSize := int64(0)
// 				for i := nextSequence; i < batchEnd; i++ {
// 					index := i & c.rb.bufferMask
// 					if c.rb.multiConsume || atomic.CompareAndSwapInt64(&c.rb.consumedSequences[index], -1, int64(consumerIndex)) {
// 						src := (*interface{})(unsafe.Pointer(uintptr(c.rb.buffer) + uintptr(index)*c.rb.elementSize))
// 						eventsBatch[batchSize] = *src
// 						batchSize++
// 						c.rb.MarkConsumed(i)
// 					}
// 				}

// 				if batchSize > 0 {
// 					atomic.AddUint64(&c.rb.totalBatches, 1)
// 					if uint64(batchSize) > atomic.LoadUint64(&c.rb.maxBatchSize) {
// 						atomic.StoreUint64(&c.rb.maxBatchSize, uint64(batchSize))
// 					}
// 					c.handler(eventsBatch[:batchSize])
// 				} else {
// 					atomic.AddUint64(&c.rb.consumerStalls, 1)
// 				}
// 				nextSequence = batchEnd
// 			}

// 			c.sequence.Set(availableSequence)
// 		}

//			// Check context again to ensure timely exit
//			select {
//			case <-c.rb.ctx.Done():
//				return
//			default:
//			}
//		}
//	}
func (c *Consumer) run(consumerIndex int) {
	nextSequence := c.sequence.Get() + 1
	eventsBatch := make([]interface{}, consumerBatchSize)

	for {
		select {
		case <-c.rb.ctx.Done():
			fmt.Printf("Consumer %d exiting due to context cancellation\n", consumerIndex)
			return
		default:
			availableSequence := c.waitStrategy.WaitFor(nextSequence, c.rb.cursor, c.sequence)

			for nextSequence <= availableSequence {
				batchEnd := nextSequence + consumerBatchSize
				if batchEnd > availableSequence+1 {
					batchEnd = availableSequence + 1
				}

				batchSize := int64(0)
				for i := nextSequence; i < batchEnd; i++ {
					index := i & c.rb.bufferMask
					if c.rb.multiConsume || atomic.CompareAndSwapInt64(&c.rb.consumedSequences[index], -1, int64(consumerIndex)) {
						src := (*interface{})(unsafe.Pointer(uintptr(c.rb.buffer) + uintptr(index)*c.rb.elementSize))
						eventsBatch[batchSize] = *src
						batchSize++
						c.rb.MarkConsumed(i)
					}
				}

				if batchSize > 0 {
					atomic.AddUint64(&c.rb.totalBatches, 1)
					if uint64(batchSize) > atomic.LoadUint64(&c.rb.maxBatchSize) {
						atomic.StoreUint64(&c.rb.maxBatchSize, uint64(batchSize))
					}
					c.handler(eventsBatch[:batchSize])
				} else {
					atomic.AddUint64(&c.rb.consumerStalls, 1)
				}
				nextSequence = batchEnd
			}

			c.sequence.Set(availableSequence)
		}
	}
}

// BusySpinWaitStrategy for ultra-low latency
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

func (BusySpinWaitStrategy) TryWaitFor(sequence int64, cursor *Sequence) bool {
	return cursor.Get() >= sequence
}

func (BusySpinWaitStrategy) SignalAllWhenBlocking() {}

// YieldingWaitStrategy with adaptive spinning
type YieldingWaitStrategy struct {
	spinTries uint32
}

func NewYieldingWaitStrategy() *YieldingWaitStrategy {
	return &YieldingWaitStrategy{spinTries: 100}
}

func (y *YieldingWaitStrategy) WaitFor(sequence int64, cursor *Sequence, dependents ...*Sequence) int64 {
	counter := y.spinTries

	for {
		availableSequence := cursor.Get()
		if availableSequence >= sequence {
			return availableSequence
		}

		counter--
		if counter == 0 {
			runtime.Gosched()
			counter = y.spinTries
		} else {
			runtime.Gosched()
		}
	}
}

func (y *YieldingWaitStrategy) TryWaitFor(sequence int64, cursor *Sequence) bool {
	counter := y.spinTries

	for counter > 0 {
		availableSequence := cursor.Get()
		if availableSequence >= sequence {
			return true
		}
		counter--
		runtime.Gosched()
	}
	return false
}

func (y *YieldingWaitStrategy) SignalAllWhenBlocking() {}

// SleepingWaitStrategy for CPU-friendly waiting
type SleepingWaitStrategy struct {
	retries int32
}

func NewSleepingWaitStrategy() *SleepingWaitStrategy {
	return &SleepingWaitStrategy{retries: 200}
}

func (s *SleepingWaitStrategy) WaitFor(sequence int64, cursor *Sequence, dependents ...*Sequence) int64 {
	counter := s.retries

	for {
		availableSequence := cursor.Get()
		if availableSequence >= sequence {
			return availableSequence
		}

		counter--
		if counter <= 0 {
			time.Sleep(1 * time.Millisecond)
		} else if counter < 100 {
			runtime.Gosched()
		}
	}
}

func (s *SleepingWaitStrategy) TryWaitFor(sequence int64, cursor *Sequence) bool {
	counter := s.retries

	for counter > 0 {
		availableSequence := cursor.Get()
		if availableSequence >= sequence {
			return true
		}
		counter--
		if counter < 100 {
			runtime.Gosched()
		}
	}
	return false
}

func (s *SleepingWaitStrategy) SignalAllWhenBlocking() {}

// BlockingWaitStrategy using conditional variables
type BlockingWaitStrategy struct {
	mutex sync.Mutex
	cond  *sync.Cond
}

func NewBlockingWaitStrategy() *BlockingWaitStrategy {
	s := &BlockingWaitStrategy{}
	s.cond = sync.NewCond(&s.mutex)
	return s
}

func (b *BlockingWaitStrategy) WaitFor(sequence int64, cursor *Sequence, dependents ...*Sequence) int64 {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	for {
		availableSequence := cursor.Get()
		if availableSequence >= sequence {
			return availableSequence
		}
		b.cond.Wait()
	}
}

func (b *BlockingWaitStrategy) TryWaitFor(sequence int64, cursor *Sequence) bool {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	availableSequence := cursor.Get()
	if availableSequence >= sequence {
		return true
	}
	return false
}

func (b *BlockingWaitStrategy) SignalAllWhenBlocking() {
	b.cond.Broadcast()
}

type Msg struct{}

var produced uint64
var consumed uint64

func runTest(name string, bufferSize int64, waitStrategy WaitStrategy, consumerCount int, multiConsume bool, useBatchPublish bool) {
	fmt.Printf("Running test: %s (MultiConsume: %v, BatchPublish: %v)\n", name, multiConsume, useBatchPublish)
	var nowTotal time.Time = time.Now()
	ringBuffer := NewRingBuffer(bufferSize, reflect.TypeOf(Msg{}), consumerCount, multiConsume)
	producer := NewProducer(ringBuffer, waitStrategy)

	ctx, cancel := context.WithTimeout(ringBuffer.ctx, 10*time.Second)
	defer cancel()
	var now time.Time

	// Start consumers
	for i := 0; i < consumerCount; i++ {
		consumer := NewConsumer(ringBuffer, func(events []interface{}) {
			atomic.AddUint64(&consumed, uint64(len(events)))
		}, waitStrategy)
		consumer.Start()
	}

	// Start producer
	producerDone := make(chan struct{})
	go func() {
		defer close(producerDone)
		localProduced := uint64(0)
		if useBatchPublish {
			batch := make([]interface{}, producerBatchSize)
			for {
				select {
				case <-ctx.Done():
					atomic.AddUint64(&produced, localProduced)
					fmt.Printf("Producer finished, produced: %d\n", localProduced)
					return
				default:
					for i := range batch {
						batch[i] = Msg{}
					}
					producer.PublishBatch(batch)
					localProduced += uint64(len(batch))
				}
			}
		} else {
			for {
				select {
				case <-ctx.Done():
					atomic.AddUint64(&produced, localProduced)
					fmt.Printf("Producer finished, produced: %d\n", localProduced)
					return
				default:
					producer.Publish(Msg{})
					localProduced++
				}
			}
		}
	}()

	// Wait for the test duration
	<-ctx.Done()
	fmt.Printf("Test %s completed, waiting for producer to finish...\n", name)
	<-producerDone
	fmt.Printf("Producer finished, waiting for consumers to finish...\n")
	now = time.Now()

	// Shutdown and wait for all consumers to finish
	ringBuffer.Shutdown()

	// Wait for all consumers to finish
	ringBuffer.Wait()

	producedFinal := atomic.LoadUint64(&produced)
	consumedFinal := atomic.LoadUint64(&consumed)

	fmt.Printf("  Produced: %d\n", producedFinal)
	fmt.Printf("  Consumed: %d\n", consumedFinal)
	fmt.Printf("  Last Produced Sequence: %d\n", ringBuffer.lastProducedSequence)
	fmt.Printf("  Last Consumed Sequence: %d\n", ringBuffer.lastConsumedSequence)
	fmt.Printf("  Cursor: %d\n", ringBuffer.cursor.Get())
	fmt.Printf("  Producer Stalls: %d\n", atomic.LoadUint64(&ringBuffer.producerStalls))
	fmt.Printf("  Consumer Stalls: %d\n", atomic.LoadUint64(&ringBuffer.consumerStalls))
	fmt.Printf("  Max Batch Size: %d\n", atomic.LoadUint64(&ringBuffer.maxBatchSize))
	fmt.Printf("  Total Batches: %d\n", atomic.LoadUint64(&ringBuffer.totalBatches))

	minConsumedSequence := ringBuffer.consumedSequences[0]
	for i := 1; i < len(ringBuffer.consumedSequences); i++ {
		if ringBuffer.consumedSequences[i] < minConsumedSequence {
			minConsumedSequence = ringBuffer.consumedSequences[i]
		}
	}
	fmt.Printf("  Min Consumed Sequence: %d\n", minConsumedSequence)

	if consumedFinal > producedFinal {
		fmt.Printf("  WARNING: Consumed count exceeds Produced count by %d\n", consumedFinal-producedFinal)
	}

	fmt.Printf("  Throughput: %.2f million ops/sec\n\n", float64(producedFinal)/10.0)
	fmt.Printf("  Last consumption duration: %v\n", time.Since(now))
	fmt.Printf("  Total duration: %v\n", time.Since(nowTotal))

	// Reset for next test
	atomic.StoreUint64(&produced, 0)
	atomic.StoreUint64(&consumed, 0)
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
		{"BusySpinWaitStrategy (1 consumer)", 1024 * 1024 * 64, &BusySpinWaitStrategy{}, 1},
		{"BusySpinWaitStrategy (2 consumers)", 1024 * 1024 * 64, &BusySpinWaitStrategy{}, 2},
		{"YieldingWaitStrategy (1 consumer)", 1024 * 1024 * 64, NewYieldingWaitStrategy(), 1},
		{"YieldingWaitStrategy (2 consumers)", 1024 * 1024 * 64, NewYieldingWaitStrategy(), 2},
		{"SleepingWaitStrategy (1 consumer)", 1024 * 1024 * 64, NewSleepingWaitStrategy(), 1},
		{"SleepingWaitStrategy (2 consumers)", 1024 * 1024 * 64, NewSleepingWaitStrategy(), 2},
		{"BlockingWaitStrategy (1 consumer)", 1024 * 1024 * 64, NewBlockingWaitStrategy(), 1},
		{"BlockingWaitStrategy (2 consumers)", 1024 * 1024 * 64, NewBlockingWaitStrategy(), 2},
	}

	for _, test := range tests {
		runTest(test.name, test.bufferSize, test.waitStrategy, test.consumerCount, false, true)
		runTest(test.name, test.bufferSize, test.waitStrategy, test.consumerCount, true, true)
		// runTest(test.name, test.bufferSize, test.waitStrategy, test.consumerCount, false, false)
		// runTest(test.name, test.bufferSize, test.waitStrategy, test.consumerCount, true, false)
	}
}
