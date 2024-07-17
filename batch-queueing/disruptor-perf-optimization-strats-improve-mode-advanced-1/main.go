package main

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

const (
	cacheLine         = 64
	producerBatchSize = 8192
	consumerBatchSize = 8192
)

type padding [cacheLine]byte

type RingBufferMode int

const (
	DispatchMode RingBufferMode = iota
	BroadcastMode
)

type Sequence struct {
	value atomic.Uint64
	_     padding
}

func NewSequence(initial uint64) *Sequence {
	s := &Sequence{}
	s.value.Store(initial)
	return s
}

//go:inline
func (s *Sequence) Get() uint64 {
	return s.value.Load()
}

//go:inline
func (s *Sequence) Set(value uint64) {
	s.value.Store(value)
}

//go:inline
func (s *Sequence) IncrementAndGet() uint64 {
	return s.value.Add(1) - 1
}

type Event struct {
	value atomic.Value
	_     [56]byte // Padding to make Event 64 bytes (cache line size)
}

type DisruptorInfo interface {
	GetSingleBufferSize() uint64
	GetBatchBufferSize() uint64
	GetSingleCurrentSequence() uint64
	GetBatchCurrentSequence() uint64
	GetConsumerSequences() []uint64
	GetMode() RingBufferMode
}

type WaitStrategy interface {
	Wait(sequence uint64, cursor *Sequence, dependentSequence *Sequence, barrier Sequencer) uint64
	SignalAllWhenBlocking()
}

type SinglePublishStrategy interface {
	Initialize(d DisruptorInfo) error
	Update(d DisruptorInfo)
	GetOptimalConsumeBatchSize() int
	ShouldApplyBackpressure() bool
}

type BatchPublishStrategy interface {
	Initialize(d DisruptorInfo) error
	Update(d DisruptorInfo)
	GetOptimalPublishBatchSize() int
	GetMaxWaitTime() time.Duration
}

type Sequencer interface {
	Next(n uint64) uint64
	Publish(lo, hi uint64)
	IsAvailable(sequence uint64) bool
	GetHighestPublishedSequence(nextSequence, availableSequence uint64) uint64
}

type SingleRingBuffer struct {
	buffer    []Event
	mask      uint64
	sequence  *Sequence
	_padding0 padding
}

func NewSingleRingBuffer(size uint64) *SingleRingBuffer {
	return &SingleRingBuffer{
		buffer:   make([]Event, size),
		mask:     size - 1,
		sequence: NewSequence(0), // keep it at `0` and not ^uint64(0) otherwise it will block the runtime
	}
}

type BatchRingBuffer struct {
	buffer    []Event
	mask      uint64
	sequence  *Sequence
	_padding0 padding
}

func NewBatchRingBuffer(size uint64) *BatchRingBuffer {
	return &BatchRingBuffer{
		buffer:   make([]Event, size),
		mask:     size - 1,
		sequence: NewSequence(0), // keep it at `0` and not ^uint64(0) otherwise it will block the runtime
	}
}

type Disruptor struct {
	batchRingBuffer   *BatchRingBuffer
	singleRingBuffer  *SingleRingBuffer
	consumers         []*Consumer
	producers         []*Producer
	waitStrategy      WaitStrategy
	singleStrategy    SinglePublishStrategy
	batchStrategy     BatchPublishStrategy
	mode              RingBufferMode
	updateInterval    time.Duration
	shutdownChan      chan struct{}
	wg                sync.WaitGroup
	producersShutdown atomic.Bool
	consumersShutdown atomic.Bool
}

//go:inline
func (d *Disruptor) GetSingleBufferSize() uint64 { return uint64(len(d.singleRingBuffer.buffer)) }

//go:inline
func (d *Disruptor) GetBatchBufferSize() uint64 { return uint64(len(d.batchRingBuffer.buffer)) }

//go:inline
func (d *Disruptor) GetSingleCurrentSequence() uint64 { return d.singleRingBuffer.sequence.Get() }

//go:inline
func (d *Disruptor) GetBatchCurrentSequence() uint64 { return d.batchRingBuffer.sequence.Get() }

//go:inline
func (d *Disruptor) GetConsumerSequences() []uint64 {
	sequences := make([]uint64, len(d.consumers))
	for i, consumer := range d.consumers {
		sequences[i] = consumer.sequence.Get()
	}
	return sequences
}

//go:inline
func (d *Disruptor) GetMode() RingBufferMode { return d.mode }

type DisruptorConfig struct {
	BatchBufferSize  uint64
	SingleBufferSize uint64
	WaitStrategy     WaitStrategy
	SingleStrategy   SinglePublishStrategy
	BatchStrategy    BatchPublishStrategy
	Mode             RingBufferMode
	UpdateInterval   time.Duration
	ConsumerCount    int
	ProducerCount    int
}

func NewDisruptor(config DisruptorConfig) (*Disruptor, error) {
	d := &Disruptor{
		waitStrategy:   config.WaitStrategy,
		singleStrategy: config.SingleStrategy,
		batchStrategy:  config.BatchStrategy,
		mode:           config.Mode,
		updateInterval: config.UpdateInterval,
		shutdownChan:   make(chan struct{}),
	}

	d.batchRingBuffer = NewBatchRingBuffer(config.BatchBufferSize)
	d.singleRingBuffer = NewSingleRingBuffer(config.SingleBufferSize)

	for i := 0; i < config.ConsumerCount; i++ {
		d.consumers = append(d.consumers, NewConsumer(d, i))
	}

	for i := 0; i < config.ProducerCount; i++ {
		d.producers = append(d.producers, NewProducer(d))
	}

	if err := d.singleStrategy.Initialize(d); err != nil {
		return nil, fmt.Errorf("failed to initialize single publish strategy: %w", err)
	}

	if err := d.batchStrategy.Initialize(d); err != nil {
		return nil, fmt.Errorf("failed to initialize batch publish strategy: %w", err)
	}

	if config.ConsumerCount < 1 {
		return nil, fmt.Errorf("at least one consumer is required")
	}

	return d, nil
}

func (d *Disruptor) Start() {
	for _, c := range d.consumers {
		d.wg.Add(2)
		if d.mode == DispatchMode {
			go c.processBatchesDispatch()
			go c.processSinglePublishDispatch()
		} else {
			go c.processBatchesBroadcast()
			go c.processSinglePublishBroadcast()
		}
	}
	go d.updateStrategies()
}

func (d *Disruptor) updateStrategies() {
	ticker := time.NewTicker(d.updateInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			d.singleStrategy.Update(d)
			d.batchStrategy.Update(d)
		case <-d.shutdownChan:
			return
		}
	}
}

func (d *Disruptor) Shutdown() {
	d.producersShutdown.Store(true)
	// Wait for producers to finish
	d.consumersShutdown.Store(true)
	close(d.shutdownChan)
}

func (d *Disruptor) Wait() {
	d.wg.Wait()
}

type Producer struct {
	disruptor *Disruptor
}

func NewProducer(d *Disruptor) *Producer {
	return &Producer{disruptor: d}
}

//go:inline
func (p *Producer) PublishSingle(value interface{}) bool {
	if p.disruptor.singleStrategy.ShouldApplyBackpressure() {
		return false
	}
	sequence := p.disruptor.singleRingBuffer.sequence.IncrementAndGet()
	event := &p.disruptor.singleRingBuffer.buffer[sequence&p.disruptor.singleRingBuffer.mask]
	event.value.Store(value)
	return true
}

//go:inline
func (p *Producer) PublishBatch(batch []interface{}) bool {
	optimalSize := p.disruptor.batchStrategy.GetOptimalPublishBatchSize()
	if len(batch) > optimalSize {
		batch = batch[:optimalSize]
	}
	n := uint64(len(batch))
	startSequence := p.disruptor.batchRingBuffer.sequence.Get() + 1
	endSequence := startSequence + n - 1

	for i, value := range batch {
		event := &p.disruptor.batchRingBuffer.buffer[(startSequence+uint64(i))&p.disruptor.batchRingBuffer.mask]
		event.value.Store(value)
	}
	p.disruptor.batchRingBuffer.sequence.Set(endSequence)
	return true
}

type Consumer struct {
	disruptor *Disruptor
	id        int
	sequence  *Sequence
}

func NewConsumer(d *Disruptor, id int) *Consumer {
	return &Consumer{
		disruptor: d,
		id:        id,
		sequence:  NewSequence(0), // keep it at `0` and not ^uint64(0) otherwise it will block the runtime
	}
}

func (c *Consumer) processBatchesDispatch() {
	defer c.disruptor.wg.Done()
	for !c.disruptor.consumersShutdown.Load() {
		nextSequence := c.sequence.Get() + 1
		availableSequence := c.disruptor.batchRingBuffer.sequence.Get()

		for nextSequence <= availableSequence {
			if nextSequence%uint64(len(c.disruptor.consumers)) == uint64(c.id) {
				event := &c.disruptor.batchRingBuffer.buffer[nextSequence&c.disruptor.batchRingBuffer.mask]
				_ = event.value.Load()
				c.sequence.Set(nextSequence)
			}
			nextSequence++
		}
	}
}

func (c *Consumer) processBatchesBroadcast() {
	defer c.disruptor.wg.Done()
	for !c.disruptor.consumersShutdown.Load() {
		nextSequence := c.sequence.Get() + 1
		availableSequence := c.disruptor.batchRingBuffer.sequence.Get()

		for nextSequence <= availableSequence {
			event := &c.disruptor.batchRingBuffer.buffer[nextSequence&c.disruptor.batchRingBuffer.mask]
			// we need to make sure the value is read somehow
			_ = event.value.Load()
			nextSequence++
		}

		c.sequence.Set(availableSequence)
	}
}

func (c *Consumer) processSinglePublishDispatch() {
	defer c.disruptor.wg.Done()
	for !c.disruptor.consumersShutdown.Load() {
		nextSequence := c.sequence.Get() + 1
		availableSequence := c.disruptor.singleRingBuffer.sequence.Get()

		batchSize := c.disruptor.singleStrategy.GetOptimalConsumeBatchSize()
		for i := 0; i < batchSize && nextSequence <= availableSequence; i++ {
			if nextSequence%uint64(len(c.disruptor.consumers)) == uint64(c.id) {
				event := &c.disruptor.singleRingBuffer.buffer[nextSequence&c.disruptor.singleRingBuffer.mask]
				// we need to make sure the value is read somehow
				_ = event.value.Load()
			}
			nextSequence++
		}

		c.sequence.Set(nextSequence - 1)
	}
}

func (c *Consumer) processSinglePublishBroadcast() {
	defer c.disruptor.wg.Done()
	for !c.disruptor.consumersShutdown.Load() {
		nextSequence := c.sequence.Get() + 1
		availableSequence := c.disruptor.singleRingBuffer.sequence.Get()

		batchSize := c.disruptor.singleStrategy.GetOptimalConsumeBatchSize()
		for i := 0; i < batchSize && nextSequence <= availableSequence; i++ {
			event := &c.disruptor.singleRingBuffer.buffer[nextSequence&c.disruptor.singleRingBuffer.mask]
			// we need to make sure the value is read somehow
			_ = event.value.Load()
			nextSequence++
		}

		c.sequence.Set(nextSequence - 1)
	}
}

// Wait Strategies

type BusySpinWaitStrategy struct{}

//go:inline
func (BusySpinWaitStrategy) Wait(sequence uint64, cursor *Sequence, dependentSequence *Sequence, barrier Sequencer) uint64 {
	for {
		if cursorValue := cursor.Get(); cursorValue >= sequence {
			return cursorValue
		}
		runtime.Gosched()
	}
}

//go:inline
func (BusySpinWaitStrategy) SignalAllWhenBlocking() {}

type YieldingWaitStrategy struct {
	spinTries int32
}

func NewYieldingWaitStrategy() *YieldingWaitStrategy {
	return &YieldingWaitStrategy{spinTries: 100}
}

//go:inline
func (y *YieldingWaitStrategy) Wait(sequence uint64, cursor *Sequence, dependentSequence *Sequence, barrier Sequencer) uint64 {
	spinTries := y.spinTries

	for {
		if cursorValue := cursor.Get(); cursorValue >= sequence {
			return cursorValue
		}

		if spinTries > 0 {
			spinTries--
			runtime.Gosched()
		} else {
			time.Sleep(time.Nanosecond)
		}
	}
}

//go:inline
func (y *YieldingWaitStrategy) SignalAllWhenBlocking() {}

type SleepingWaitStrategy struct {
	retries        int32
	sleepTimeNs    int64
	maxSleepTimeNs int64
}

func NewSleepingWaitStrategy(retries int32, sleepTimeNs, maxSleepTimeNs int64) *SleepingWaitStrategy {
	return &SleepingWaitStrategy{
		retries:        retries,
		sleepTimeNs:    sleepTimeNs,
		maxSleepTimeNs: maxSleepTimeNs,
	}
}

//go:inline
func (s *SleepingWaitStrategy) Wait(sequence uint64, cursor *Sequence, dependentSequence *Sequence, barrier Sequencer) uint64 {
	retries := s.retries

	for {
		if cursorValue := cursor.Get(); cursorValue >= sequence {
			return cursorValue
		}

		if retries > 0 {
			retries--
		} else {
			sleepTime := time.Duration(math.Min(float64(s.sleepTimeNs), float64(s.maxSleepTimeNs)))
			time.Sleep(sleepTime)
			s.sleepTimeNs = int64(math.Min(float64(s.maxSleepTimeNs), float64(s.sleepTimeNs)*2))
		}
	}
}

//go:inline
func (s *SleepingWaitStrategy) SignalAllWhenBlocking() {}

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

//go:inline
func (b *BlockingWaitStrategy) Wait(sequence uint64, cursor *Sequence, dependentSequence *Sequence, barrier Sequencer) uint64 {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	for {
		if cursorValue := cursor.Get(); cursorValue >= sequence {
			return cursorValue
		}
		b.cond.Wait()
	}
}

//go:inline
func (b *BlockingWaitStrategy) SignalAllWhenBlocking() {
	b.cond.Broadcast()
}

// Single Publish Strategies

type SimplePublishStrategy struct {
	batchSize int
}

func NewSimplePublishStrategy(batchSize int) *SimplePublishStrategy {
	return &SimplePublishStrategy{batchSize: batchSize}
}

func (s *SimplePublishStrategy) Initialize(d DisruptorInfo) error { return nil }
func (s *SimplePublishStrategy) Update(d DisruptorInfo)           {}
func (s *SimplePublishStrategy) GetOptimalConsumeBatchSize() int  { return s.batchSize }
func (s *SimplePublishStrategy) ShouldApplyBackpressure() bool    { return false }

type AdaptiveBatchingStrategy struct {
	currentBatchSize atomic.Int32
	minBatchSize     int32
	maxBatchSize     int32
}

func NewAdaptiveBatchingStrategy(minBatchSize, maxBatchSize int) *AdaptiveBatchingStrategy {
	return &AdaptiveBatchingStrategy{
		minBatchSize: int32(minBatchSize),
		maxBatchSize: int32(maxBatchSize),
	}
}

func (s *AdaptiveBatchingStrategy) Initialize(d DisruptorInfo) error {
	if len(d.GetConsumerSequences()) == 0 {
		return fmt.Errorf("adaptive batching strategy requires at least one consumer")
	}
	s.currentBatchSize.Store(s.minBatchSize)
	return nil
}

func (s *AdaptiveBatchingStrategy) Update(d DisruptorInfo) {
	minConsumerSequence := d.GetConsumerSequences()[0]
	for _, seq := range d.GetConsumerSequences()[1:] {
		if seq < minConsumerSequence {
			minConsumerSequence = seq
		}
	}
	occupancy := float64(d.GetSingleCurrentSequence()-minConsumerSequence) / float64(d.GetSingleBufferSize())
	current := s.currentBatchSize.Load()
	if occupancy > 0.8 {
		newSize := int32(math.Min(float64(current*2), float64(s.maxBatchSize)))
		s.currentBatchSize.Store(newSize)
	} else if occupancy < 0.2 {
		newSize := int32(math.Max(float64(current/2), float64(s.minBatchSize)))
		s.currentBatchSize.Store(newSize)
	}
}

//go:inline
func (s *AdaptiveBatchingStrategy) GetOptimalConsumeBatchSize() int {
	return int(s.currentBatchSize.Load())
}

//go:inline
func (s *AdaptiveBatchingStrategy) ShouldApplyBackpressure() bool {
	return s.currentBatchSize.Load() == s.maxBatchSize
}

type TimeBasedFlushingStrategy struct {
	flushInterval time.Duration
	lastFlushTime time.Time
	batchSize     int
}

func NewTimeBasedFlushingStrategy(flushInterval time.Duration, batchSize int) *TimeBasedFlushingStrategy {
	return &TimeBasedFlushingStrategy{
		flushInterval: flushInterval,
		lastFlushTime: time.Now(),
		batchSize:     batchSize,
	}
}

func (s *TimeBasedFlushingStrategy) Initialize(d DisruptorInfo) error { return nil }

func (s *TimeBasedFlushingStrategy) Update(d DisruptorInfo) {
	if time.Since(s.lastFlushTime) >= s.flushInterval {
		s.lastFlushTime = time.Now()
	}
}

//go:inline
func (s *TimeBasedFlushingStrategy) GetOptimalConsumeBatchSize() int {
	return s.batchSize
}

//go:inline
func (s *TimeBasedFlushingStrategy) ShouldApplyBackpressure() bool {
	return false
}

type BackpressureStrategy struct {
	threshold     float64
	currentBuffer atomic.Value // Will store DisruptorInfo
	mu            sync.Mutex   // To protect Update and ShouldApplyBackpressure
}

func NewBackpressureStrategy(threshold float64) *BackpressureStrategy {
	return &BackpressureStrategy{
		threshold: threshold,
	}
}

func (s *BackpressureStrategy) Initialize(d DisruptorInfo) error {
	if len(d.GetConsumerSequences()) == 0 {
		return fmt.Errorf("backpressure strategy requires at least one consumer")
	}
	s.currentBuffer.Store(d)
	return nil
}

func (s *BackpressureStrategy) Update(d DisruptorInfo) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.currentBuffer.Store(d)
}

//go:inline
func (s *BackpressureStrategy) GetOptimalConsumeBatchSize() int {
	return 100 // Example fixed batch size
}

//go:inline
func (s *BackpressureStrategy) ShouldApplyBackpressure() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	d, ok := s.currentBuffer.Load().(DisruptorInfo)
	if !ok || d == nil {
		return false
	}
	sequences := d.GetConsumerSequences()
	if len(sequences) == 0 {
		return false // No consumers, so no backpressure
	}
	minConsumerSequence := sequences[0]
	for _, seq := range sequences[1:] {
		if seq < minConsumerSequence {
			minConsumerSequence = seq
		}
	}
	occupancy := float64(d.GetSingleCurrentSequence()-minConsumerSequence) / float64(d.GetSingleBufferSize())
	return occupancy > s.threshold
}

// Batch Publish Strategies

type SimpleBatchStrategy struct {
	batchSize   int
	maxWaitTime time.Duration
}

func NewSimpleBatchStrategy(batchSize int, maxWaitTime time.Duration) *SimpleBatchStrategy {
	return &SimpleBatchStrategy{
		batchSize:   batchSize,
		maxWaitTime: maxWaitTime,
	}
}

func (s *SimpleBatchStrategy) Initialize(d DisruptorInfo) error { return nil }
func (s *SimpleBatchStrategy) Update(d DisruptorInfo)           {}

//go:inline
func (s *SimpleBatchStrategy) GetOptimalPublishBatchSize() int { return s.batchSize }

//go:inline
func (s *SimpleBatchStrategy) GetMaxWaitTime() time.Duration { return s.maxWaitTime }

type AdaptiveBatchPublishStrategy struct {
	currentBatchSize atomic.Int32
	minBatchSize     int32
	maxBatchSize     int32
	maxWaitTime      time.Duration
}

func NewAdaptiveBatchPublishStrategy(minBatchSize, maxBatchSize int, maxWaitTime time.Duration) *AdaptiveBatchPublishStrategy {
	return &AdaptiveBatchPublishStrategy{
		minBatchSize: int32(minBatchSize),
		maxBatchSize: int32(maxBatchSize),
		maxWaitTime:  maxWaitTime,
	}
}

func (s *AdaptiveBatchPublishStrategy) Initialize(d DisruptorInfo) error {
	if len(d.GetConsumerSequences()) == 0 {
		return fmt.Errorf("adaptive batching strategy requires at least one consumer")
	}
	s.currentBatchSize.Store(s.minBatchSize)
	return nil
}

func (s *AdaptiveBatchPublishStrategy) Update(d DisruptorInfo) {
	minConsumerSequence := d.GetConsumerSequences()[0]
	for _, seq := range d.GetConsumerSequences()[1:] {
		if seq < minConsumerSequence {
			minConsumerSequence = seq
		}
	}
	occupancy := float64(d.GetBatchCurrentSequence()-minConsumerSequence) / float64(d.GetBatchBufferSize())
	current := s.currentBatchSize.Load()
	if occupancy > 0.8 {
		newSize := int32(math.Min(float64(current*2), float64(s.maxBatchSize)))
		s.currentBatchSize.Store(newSize)
	} else if occupancy < 0.2 {
		newSize := int32(math.Max(float64(current/2), float64(s.minBatchSize)))
		s.currentBatchSize.Store(newSize)
	}
}

//go:inline
func (s *AdaptiveBatchPublishStrategy) GetOptimalPublishBatchSize() int {
	return int(s.currentBatchSize.Load())
}

//go:inline
func (s *AdaptiveBatchPublishStrategy) GetMaxWaitTime() time.Duration {
	return s.maxWaitTime
}

func runTest(name string, config DisruptorConfig, duration time.Duration) {
	fmt.Printf("Running test: %s\n", name)

	disruptor, err := NewDisruptor(config)
	if err != nil {
		fmt.Printf("Error creating disruptor: %v\n", err)
		return
	}

	disruptor.Start()

	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	var totalProduced, totalConsumed atomic.Uint64

	// Start producers
	for _, p := range disruptor.producers {
		go func(prod *Producer) {
			for {
				select {
				case <-ctx.Done():
					return
				default:
					if rand.Float32() < 0.5 {
						value := rand.Int63()
						if prod.PublishSingle(value) {
							totalProduced.Add(1)
						}
					} else {
						batch := make([]interface{}, rand.Intn(producerBatchSize)+1)
						for i := range batch {
							batch[i] = rand.Int63()
						}
						if prod.PublishBatch(batch) {
							totalProduced.Add(uint64(len(batch)))
						}
					}
				}
			}
		}(p)
	}

	// Start a goroutine to periodically update totalConsumed
	go func() {
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				var consumed uint64
				for _, c := range disruptor.consumers {
					consumed += c.sequence.Get() + 1
				}
				totalConsumed.Store(consumed)
			}
		}
	}()

	<-ctx.Done()
	disruptor.Shutdown()
	disruptor.Wait()

	produced := totalProduced.Load()
	consumed := totalConsumed.Load()

	fmt.Printf("Total produced: %d\n", produced)
	fmt.Printf("Total consumed: %d\n", consumed)
	fmt.Printf("Producer throughput: %.2f million ops/sec\n", float64(produced)/duration.Seconds()/1e6)
	fmt.Printf("Consumer throughput: %.2f million ops/sec\n\n", float64(consumed)/duration.Seconds()/1e6)
}

func main() {
	waitStrategies := []WaitStrategy{
		&BusySpinWaitStrategy{},
		NewYieldingWaitStrategy(),
		NewSleepingWaitStrategy(100, 100, 1000000),
		NewBlockingWaitStrategy(),
	}

	singlePublishStrategies := []SinglePublishStrategy{
		NewSimplePublishStrategy(100),
		NewAdaptiveBatchingStrategy(10, 1000),
		NewTimeBasedFlushingStrategy(100*time.Millisecond, 100),
		NewBackpressureStrategy(0.8),
	}

	batchPublishStrategies := []BatchPublishStrategy{
		NewSimpleBatchStrategy(1000, 10*time.Millisecond),
		NewAdaptiveBatchPublishStrategy(100, 10000, 10*time.Millisecond),
	}

	// bufferSizes := []uint64{1024 * 1024 * 64, 1024, 4096, 16384, 65536}
	// modes := []RingBufferMode{DispatchMode, BroadcastMode}
	// workerCounts := []int{1, 2, 4, 8}

	// for _, mode := range modes {
	// 	for _, batchSize := range bufferSizes {
	// 		for _, singleSize := range bufferSizes {
	// 			for _, producerCount := range workerCounts {
	// 				for _, consumerCount := range workerCounts {
	// 					for _, waitStrategy := range waitStrategies {
	// 						for _, singleStrategy := range singlePublishStrategies {
	// 							for _, batchStrategy := range batchPublishStrategies {
	// 								config := DisruptorConfig{
	// 									BatchBufferSize:  batchSize,
	// 									SingleBufferSize: singleSize,
	// 									WaitStrategy:     waitStrategy,
	// 									SingleStrategy:   singleStrategy,
	// 									BatchStrategy:    batchStrategy,
	// 									Mode:             mode,
	// 									UpdateInterval:   time.Second,
	// 									ConsumerCount:    consumerCount,
	// 									ProducerCount:    producerCount,
	// 								}
	// 								testName := fmt.Sprintf("Mode: %v, BatchSize: %d, SingleSize: %d, Producers: %d, Consumers: %d, WaitStrategy: %T, SingleStrategy: %T, BatchStrategy: %T",
	// 									mode, batchSize, singleSize, producerCount, consumerCount, waitStrategy, singleStrategy, batchStrategy)
	// 								runTest(testName, config, 5*time.Second)
	// 							}
	// 						}
	// 					}
	// 				}
	// 			}
	// 		}
	// 	}
	// }

	bufferSizes := []uint64{1024 * 1024 * 64}
	modes := []RingBufferMode{DispatchMode}
	workerCounts := []int{1, 2}

	for _, mode := range modes {
		for _, batchSize := range bufferSizes {
			for _, singleSize := range bufferSizes {
				for _, producerCount := range workerCounts {
					for _, consumerCount := range workerCounts {
						for _, waitStrategy := range waitStrategies {
							for _, singleStrategy := range singlePublishStrategies {
								for _, batchStrategy := range batchPublishStrategies {
									config := DisruptorConfig{
										BatchBufferSize:  batchSize,
										SingleBufferSize: singleSize,
										WaitStrategy:     waitStrategy,
										SingleStrategy:   singleStrategy,
										BatchStrategy:    batchStrategy,
										Mode:             mode,
										UpdateInterval:   time.Second,
										ConsumerCount:    consumerCount,
										ProducerCount:    producerCount,
									}
									testName := fmt.Sprintf("Mode: %v, BatchSize: %d, SingleSize: %d, Producers: %d, Consumers: %d, WaitStrategy: %T, SingleStrategy: %T, BatchStrategy: %T",
										mode, batchSize, singleSize, producerCount, consumerCount, waitStrategy, singleStrategy, batchStrategy)
									runTest(testName, config, 2*time.Second)
								}
							}
						}
					}
				}
			}
		}
	}
}
