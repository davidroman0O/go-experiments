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

type RingBufferMode uint8

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

type EventPool struct {
	pool sync.Pool
}

func NewEventPool() *EventPool {
	return &EventPool{
		pool: sync.Pool{
			New: func() interface{} {
				return &Event{}
			},
		},
	}
}

func (ep *EventPool) Get() *Event {
	return ep.pool.Get().(*Event)
}

func (ep *EventPool) Put(e *Event) {
	e.value.Store(nil)
	ep.pool.Put(e)
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
	GetOptimalConsumeBatchSize() uint32
	ShouldApplyBackpressure() bool
	GetFlushInterval() time.Duration
}

type BatchPublishStrategy interface {
	Initialize(d DisruptorInfo) error
	Update(d DisruptorInfo)
	GetOptimalPublishBatchSize() uint32
	GetMaxWaitTime() time.Duration
}

type Sequencer interface {
	Next(n uint64) uint64
	Publish(lo, hi uint64)
	IsAvailable(sequence uint64) bool
	GetHighestPublishedSequence(nextSequence, availableSequence uint64) uint64
}

func logProgress(task string, current, total int) {
	// percentage := float64(current) / float64(total) * 100
	// fmt.Printf("\r%s: %.2f%% complete (%d/%d)", task, percentage, current, total)
	// if current == total {
	// 	fmt.Println()
	// }
}

type SingleRingBuffer struct {
	buffer    []*Event
	mask      uint64
	sequence  *Sequence
	eventPool *EventPool
	_padding0 padding
}

func NewSingleRingBuffer(size uint64, eventPool *EventPool) *SingleRingBuffer {
	// fmt.Printf("Creating SingleRingBuffer with size %d\n", size)
	buffer := make([]*Event, size)
	for i := range buffer {
		buffer[i] = eventPool.Get()
		if i%1000 == 0 || i == int(size)-1 {
			logProgress("SingleRingBuffer initialization", i+1, int(size))
		}
	}
	// fmt.Println("SingleRingBuffer created successfully")
	return &SingleRingBuffer{
		buffer:    buffer,
		mask:      size - 1,
		sequence:  NewSequence(0),
		eventPool: eventPool,
	}
}

type BatchRingBuffer struct {
	buffer    []*Event
	mask      uint64
	sequence  *Sequence
	eventPool *EventPool
	_padding0 padding
}

func NewBatchRingBuffer(size uint64, eventPool *EventPool) *BatchRingBuffer {
	// fmt.Printf("Creating BatchRingBuffer with size %d\n", size)
	buffer := make([]*Event, size)
	for i := range buffer {
		buffer[i] = eventPool.Get()
		if i%1000 == 0 || i == int(size)-1 {
			logProgress("BatchRingBuffer initialization", i+1, int(size))
		}
	}
	// fmt.Println("BatchRingBuffer created successfully")
	return &BatchRingBuffer{
		buffer:    buffer,
		mask:      size - 1,
		sequence:  NewSequence(0),
		eventPool: eventPool,
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
	singleEventPool   *EventPool
	batchEventPool    *EventPool
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
	ConsumerCount    uint32
	ProducerCount    uint32
	Handler          func([]interface{})
}

func NewDisruptor(config DisruptorConfig) (*Disruptor, error) {
	// fmt.Println("Creating new Disruptor")
	d := &Disruptor{
		waitStrategy:    config.WaitStrategy,
		singleStrategy:  config.SingleStrategy,
		batchStrategy:   config.BatchStrategy,
		mode:            config.Mode,
		updateInterval:  config.UpdateInterval,
		shutdownChan:    make(chan struct{}),
		singleEventPool: NewEventPool(),
		batchEventPool:  NewEventPool(),
	}
	// fmt.Println("Disruptor struct initialized")

	// fmt.Println("Creating batch ring buffer")
	d.batchRingBuffer = NewBatchRingBuffer(config.BatchBufferSize, d.batchEventPool)
	// fmt.Println("Creating single ring buffer")
	d.singleRingBuffer = NewSingleRingBuffer(config.SingleBufferSize, d.singleEventPool)

	// fmt.Printf("Creating %d consumers\n", config.ConsumerCount)
	d.consumers = make([]*Consumer, config.ConsumerCount)
	for i := uint32(0); i < config.ConsumerCount; i++ {
		d.consumers[i] = NewConsumer(d, i, config.Handler)
		logProgress("Consumer creation", int(i+1), int(config.ConsumerCount))
	}

	// fmt.Printf("Creating %d producers\n", config.ProducerCount)
	d.producers = make([]*Producer, config.ProducerCount)
	for i := uint32(0); i < config.ProducerCount; i++ {
		d.producers[i] = NewProducer(d)
		logProgress("Producer creation", int(i+1), int(config.ProducerCount))
	}

	// fmt.Println("Initializing single publish strategy")
	if err := d.singleStrategy.Initialize(d); err != nil {
		return nil, fmt.Errorf("failed to initialize single publish strategy: %w", err)
	}

	// fmt.Println("Initializing batch publish strategy")
	if err := d.batchStrategy.Initialize(d); err != nil {
		return nil, fmt.Errorf("failed to initialize batch publish strategy: %w", err)
	}

	if config.ConsumerCount < 1 {
		return nil, fmt.Errorf("at least one consumer is required")
	}

	// fmt.Println("Disruptor creation completed successfully")
	return d, nil
}

func (d *Disruptor) Start() {
	// fmt.Println("Starting consumers")
	for i, c := range d.consumers {
		d.wg.Add(2)
		if d.mode == DispatchMode {
			go func(consumer *Consumer, id int) {
				// fmt.Printf("Consumer %d (Dispatch) started\n", id)
				consumer.processBatchesDispatch()
			}(c, i)
			go func(consumer *Consumer, id int) {
				// fmt.Printf("Consumer %d (Single) started\n", id)
				consumer.processSinglePublishDispatch()
			}(c, i)
		} else {
			go func(consumer *Consumer, id int) {
				// fmt.Printf("Consumer %d (Broadcast) started\n", id)
				consumer.processBatchesBroadcast()
			}(c, i)
			go func(consumer *Consumer, id int) {
				// fmt.Printf("Consumer %d (Single Broadcast) started\n", id)
				consumer.processSinglePublishBroadcast()
			}(c, i)
		}
	}
	// fmt.Println("Starting strategy updater")
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
	event := p.disruptor.singleRingBuffer.buffer[sequence&p.disruptor.singleRingBuffer.mask]
	event.value.Store(value)
	return true
}

//go:inline
func (p *Producer) PublishBatch(batch []interface{}) bool {
	optimalSize := p.disruptor.batchStrategy.GetOptimalPublishBatchSize()
	if uint32(len(batch)) > optimalSize {
		batch = batch[:optimalSize]
	}
	n := uint64(len(batch))
	startSequence := p.disruptor.batchRingBuffer.sequence.Get() + 1
	endSequence := startSequence + n - 1

	for i, value := range batch {
		event := p.disruptor.batchRingBuffer.buffer[(startSequence+uint64(i))&p.disruptor.batchRingBuffer.mask]
		event.value.Store(value)
	}
	p.disruptor.batchRingBuffer.sequence.Set(endSequence)
	return true
}

type Consumer struct {
	disruptor   *Disruptor
	id          uint32
	sequence    *Sequence
	handler     func([]interface{})
	singleBatch []interface{}
}

func NewConsumer(d *Disruptor, id uint32, handler func([]interface{})) *Consumer {
	return &Consumer{
		disruptor:   d,
		id:          id,
		sequence:    NewSequence(0),
		handler:     handler,
		singleBatch: make([]interface{}, 0, 1024), // Pre-allocate space for batch
	}
}

func (c *Consumer) processBatchesDispatch() {
	defer c.disruptor.wg.Done()
	var processedCount uint64
	// fmt.Printf("Consumer %d: Starting processBatchesDispatch\n", c.id)
	for !c.disruptor.consumersShutdown.Load() {
		nextSequence := c.sequence.Get() + 1
		availableSequence := c.disruptor.batchRingBuffer.sequence.Get()

		if nextSequence <= availableSequence {
			batch := make([]interface{}, 0, availableSequence-nextSequence+1)
			for ; nextSequence <= availableSequence; nextSequence++ {
				if nextSequence%uint64(len(c.disruptor.consumers)) == uint64(c.id) {
					event := c.disruptor.batchRingBuffer.buffer[nextSequence&c.disruptor.batchRingBuffer.mask]
					batch = append(batch, event.value.Load())
					processedCount++
				}
			}
			if len(batch) > 0 {
				c.handler(batch)
			}
			c.sequence.Set(nextSequence - 1)
		} else {
			runtime.Gosched() // Yield to other goroutines
		}
	}
	// fmt.Printf("Consumer %d: Exiting processBatchesDispatch, processed %d events\n", c.id, processedCount)
}

func (c *Consumer) processBatchesBroadcast() {
	defer c.disruptor.wg.Done()
	for !c.disruptor.consumersShutdown.Load() {
		nextSequence := c.sequence.Get() + 1
		availableSequence := c.disruptor.batchRingBuffer.sequence.Get()

		if nextSequence <= availableSequence {
			batch := make([]interface{}, 0, availableSequence-nextSequence+1)
			for ; nextSequence <= availableSequence; nextSequence++ {
				event := c.disruptor.batchRingBuffer.buffer[nextSequence&c.disruptor.batchRingBuffer.mask]
				batch = append(batch, event.value.Load())
			}
			if len(batch) > 0 {
				c.handler(batch)
			}
			c.sequence.Set(availableSequence)
		} else {
			runtime.Gosched() // Yield to other goroutines
		}
	}
}

func (c *Consumer) processSinglePublishDispatch() {
	defer c.disruptor.wg.Done()
	for !c.disruptor.consumersShutdown.Load() {
		nextSequence := c.sequence.Get() + 1
		availableSequence := c.disruptor.singleRingBuffer.sequence.Get()

		batchSize := c.disruptor.singleStrategy.GetOptimalConsumeBatchSize()
		c.singleBatch = c.singleBatch[:0] // Reset batch

		for i := uint32(0); i < batchSize && nextSequence <= availableSequence; i++ {
			if nextSequence%uint64(len(c.disruptor.consumers)) == uint64(c.id) {
				event := c.disruptor.singleRingBuffer.buffer[nextSequence&c.disruptor.singleRingBuffer.mask]
				c.singleBatch = append(c.singleBatch, event.value.Load())
			}
			nextSequence++
		}

		if len(c.singleBatch) > 0 {
			c.handler(c.singleBatch)
		}

		c.sequence.Set(nextSequence - 1)

		// Time-based flushing for single-publish
		if len(c.singleBatch) < int(batchSize) {
			time.Sleep(c.disruptor.singleStrategy.GetFlushInterval())
		}
	}
}

func (c *Consumer) processSinglePublishBroadcast() {
	defer c.disruptor.wg.Done()
	for !c.disruptor.consumersShutdown.Load() {
		nextSequence := c.sequence.Get() + 1
		availableSequence := c.disruptor.singleRingBuffer.sequence.Get()

		batchSize := c.disruptor.singleStrategy.GetOptimalConsumeBatchSize()
		c.singleBatch = c.singleBatch[:0] // Reset batch

		for i := uint32(0); i < batchSize && nextSequence <= availableSequence; i++ {
			event := c.disruptor.singleRingBuffer.buffer[nextSequence&c.disruptor.singleRingBuffer.mask]
			c.singleBatch = append(c.singleBatch, event.value.Load())
			nextSequence++
		}

		if len(c.singleBatch) > 0 {
			c.handler(c.singleBatch)
		}

		c.sequence.Set(nextSequence - 1)

		// Time-based flushing for single-publish
		if len(c.singleBatch) < int(batchSize) {
			time.Sleep(c.disruptor.singleStrategy.GetFlushInterval())
		}
	}
}

// Wait Strategies

type BusySpinWaitStrategy struct {
	spinTries uint32
}

func NewBusySpinWaitStrategy(spinTries uint32) *BusySpinWaitStrategy {
	return &BusySpinWaitStrategy{spinTries: spinTries}
}

//go:inline
func (b *BusySpinWaitStrategy) Wait(sequence uint64, cursor *Sequence, dependentSequence *Sequence, barrier Sequencer) uint64 {
	spinTries := b.spinTries

	for {
		cursorValue := cursor.Get()
		if cursorValue >= sequence {
			return cursorValue
		}

		if spinTries > 0 {
			spinTries--
			runtime.Gosched()
		} else {
			time.Sleep(time.Nanosecond)
			spinTries = b.spinTries
		}
	}
}

//go:inline
func (BusySpinWaitStrategy) SignalAllWhenBlocking() {}

type YieldingWaitStrategy struct {
	spinTries uint32
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
	retries        uint32
	sleepTimeNs    uint64
	maxSleepTimeNs uint64
}

func NewSleepingWaitStrategy(retries uint32, sleepTimeNs, maxSleepTimeNs uint64) *SleepingWaitStrategy {
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
			s.sleepTimeNs = uint64(math.Min(float64(s.maxSleepTimeNs), float64(s.sleepTimeNs)*2))
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
	batchSize     uint32
	flushInterval time.Duration
}

func NewSimplePublishStrategy(batchSize uint32, flushInterval time.Duration) *SimplePublishStrategy {
	return &SimplePublishStrategy{batchSize: batchSize, flushInterval: flushInterval}
}

func (s *SimplePublishStrategy) Initialize(d DisruptorInfo) error   { return nil }
func (s *SimplePublishStrategy) Update(d DisruptorInfo)             {}
func (s *SimplePublishStrategy) GetOptimalConsumeBatchSize() uint32 { return s.batchSize }
func (s *SimplePublishStrategy) ShouldApplyBackpressure() bool      { return false }
func (s *SimplePublishStrategy) GetFlushInterval() time.Duration    { return s.flushInterval }

type AdaptiveBatchingStrategy struct {
	currentBatchSize atomic.Uint32
	minBatchSize     uint32
	maxBatchSize     uint32
	flushInterval    time.Duration
}

func NewAdaptiveBatchingStrategy(minBatchSize, maxBatchSize uint32, flushInterval time.Duration) *AdaptiveBatchingStrategy {
	return &AdaptiveBatchingStrategy{
		minBatchSize:  minBatchSize,
		maxBatchSize:  maxBatchSize,
		flushInterval: flushInterval,
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
		newSize := uint32(math.Min(float64(current*2), float64(s.maxBatchSize)))
		s.currentBatchSize.Store(newSize)
	} else if occupancy < 0.2 {
		newSize := uint32(math.Max(float64(current/2), float64(s.minBatchSize)))
		s.currentBatchSize.Store(newSize)
	}
}

//go:inline
func (s *AdaptiveBatchingStrategy) GetOptimalConsumeBatchSize() uint32 {
	return s.currentBatchSize.Load()
}

//go:inline
func (s *AdaptiveBatchingStrategy) ShouldApplyBackpressure() bool {
	return s.currentBatchSize.Load() == s.maxBatchSize
}

//go:inline
func (s *AdaptiveBatchingStrategy) GetFlushInterval() time.Duration {
	return s.flushInterval
}

type TimeBasedFlushingStrategy struct {
	flushInterval time.Duration
	lastFlushTime time.Time
	batchSize     uint32
}

func NewTimeBasedFlushingStrategy(flushInterval time.Duration, batchSize uint32) *TimeBasedFlushingStrategy {
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
func (s *TimeBasedFlushingStrategy) GetOptimalConsumeBatchSize() uint32 {
	return s.batchSize
}

//go:inline
func (s *TimeBasedFlushingStrategy) ShouldApplyBackpressure() bool {
	return false
}

//go:inline
func (s *TimeBasedFlushingStrategy) GetFlushInterval() time.Duration {
	return s.flushInterval
}

type BackpressureStrategy struct {
	threshold     float64
	currentBuffer atomic.Value // Will store DisruptorInfo
	mu            sync.Mutex   // To protect Update and ShouldApplyBackpressure
	batchSize     uint32
	flushInterval time.Duration
}

func NewBackpressureStrategy(threshold float64, batchSize uint32, flushInterval time.Duration) *BackpressureStrategy {
	return &BackpressureStrategy{
		threshold:     threshold,
		batchSize:     batchSize,
		flushInterval: flushInterval,
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
func (s *BackpressureStrategy) GetOptimalConsumeBatchSize() uint32 {
	return s.batchSize
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

//go:inline
func (s *BackpressureStrategy) GetFlushInterval() time.Duration {
	return s.flushInterval
}

// Batch Publish Strategies

type SimpleBatchStrategy struct {
	batchSize   uint32
	maxWaitTime time.Duration
}

func NewSimpleBatchStrategy(batchSize uint32, maxWaitTime time.Duration) *SimpleBatchStrategy {
	return &SimpleBatchStrategy{
		batchSize:   batchSize,
		maxWaitTime: maxWaitTime,
	}
}

func (s *SimpleBatchStrategy) Initialize(d DisruptorInfo) error { return nil }
func (s *SimpleBatchStrategy) Update(d DisruptorInfo)           {}

//go:inline
func (s *SimpleBatchStrategy) GetOptimalPublishBatchSize() uint32 { return s.batchSize }

//go:inline
func (s *SimpleBatchStrategy) GetMaxWaitTime() time.Duration { return s.maxWaitTime }

type AdaptiveBatchPublishStrategy struct {
	currentBatchSize atomic.Uint32
	minBatchSize     uint32
	maxBatchSize     uint32
	maxWaitTime      time.Duration
}

func NewAdaptiveBatchPublishStrategy(minBatchSize, maxBatchSize uint32, maxWaitTime time.Duration) *AdaptiveBatchPublishStrategy {
	return &AdaptiveBatchPublishStrategy{
		minBatchSize: minBatchSize,
		maxBatchSize: maxBatchSize,
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
		newSize := uint32(math.Min(float64(current*2), float64(s.maxBatchSize)))
		s.currentBatchSize.Store(newSize)
	} else if occupancy < 0.2 {
		newSize := uint32(math.Max(float64(current/2), float64(s.minBatchSize)))
		s.currentBatchSize.Store(newSize)
	}
}

//go:inline
func (s *AdaptiveBatchPublishStrategy) GetOptimalPublishBatchSize() uint32 {
	return s.currentBatchSize.Load()
}

//go:inline
func (s *AdaptiveBatchPublishStrategy) GetMaxWaitTime() time.Duration {
	return s.maxWaitTime
}

type TestMode int

const (
	SinglePublishOnly TestMode = iota
	BatchPublishOnly
	HalfSingleHalfBatch
)

func runTest(name string, config DisruptorConfig, duration time.Duration, mode TestMode) {
	fmt.Printf("Starting test: %s (Mode: %v)\n", name, mode)

	var handlerConsumed atomic.Uint64
	config.Handler = func(batch []interface{}) {
		handlerConsumed.Add(uint64(len(batch)))
	}

	// fmt.Println("Creating Disruptor")
	disruptor, err := NewDisruptor(config)
	if err != nil {
		fmt.Printf("Error creating disruptor: %v\n", err)
		return
	}

	// fmt.Println("Disruptor created successfully")
	// fmt.Println("Starting Disruptor")
	disruptor.Start()
	// fmt.Println("Disruptor started")

	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	var totalProduced atomic.Uint64

	// Start producers
	for i, p := range disruptor.producers {
		go func(prod *Producer, id int) {
			fmt.Printf("Producer %d started\n", id)
			for {
				select {
				case <-ctx.Done():
					fmt.Printf("Producer %d stopping\n", id)
					return
				default:
					switch mode {
					case SinglePublishOnly:
						value := rand.Int63()
						if prod.PublishSingle(value) {
							totalProduced.Add(1)
						}
					case BatchPublishOnly:
						batch := make([]interface{}, rand.Intn(int(producerBatchSize))+1)
						for i := range batch {
							batch[i] = rand.Int63()
						}
						if prod.PublishBatch(batch) {
							totalProduced.Add(uint64(len(batch)))
						}
					case HalfSingleHalfBatch:
						if rand.Float32() < 0.5 {
							value := rand.Int63()
							if prod.PublishSingle(value) {
								totalProduced.Add(1)
							}
						} else {
							batch := make([]interface{}, rand.Intn(int(producerBatchSize))+1)
							for i := range batch {
								batch[i] = rand.Int63()
							}
							if prod.PublishBatch(batch) {
								totalProduced.Add(uint64(len(batch)))
							}
						}
					}
				}
			}
		}(p, i)
	}

	// Monitor progress
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	startTime := time.Now()
	for {
		select {
		case <-ctx.Done():
			elapsedTime := time.Since(startTime)
			produced := totalProduced.Load()
			consumed := handlerConsumed.Load()
			fmt.Printf("Test completed: %s (Mode: %v)\n", name, mode)
			fmt.Printf("Total produced: %d\n", produced)
			fmt.Printf("Total consumed (handler): %d\n", consumed)
			fmt.Printf("Producer throughput: %.2f million ops/sec\n", float64(produced)/elapsedTime.Seconds()/1e6)
			fmt.Printf("Handler throughput: %.2f million ops/sec\n\n", float64(consumed)/elapsedTime.Seconds()/1e6)

			// fmt.Println("Initiating shutdown")
			disruptor.Shutdown()
			disruptor.Wait()
			return

		case <-ticker.C:
			produced := totalProduced.Load()
			consumed := handlerConsumed.Load()
			elapsedTime := time.Since(startTime)
			fmt.Printf("Progress - Produced: %d, Consumed: %d, Producer throughput: %.2f M ops/sec, Handler throughput: %.2f M ops/sec\n",
				produced, consumed, float64(produced)/elapsedTime.Seconds()/1e6, float64(consumed)/elapsedTime.Seconds()/1e6)
		}
	}
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	// fmt.Println("Starting main function")

	waitStrategies := []WaitStrategy{
		NewBusySpinWaitStrategy(1000),
		NewYieldingWaitStrategy(),
		NewSleepingWaitStrategy(100, 100, 1000000),
		NewBlockingWaitStrategy(),
	}

	singlePublishStrategies := []SinglePublishStrategy{
		NewSimplePublishStrategy(100, 1*time.Millisecond),
		NewAdaptiveBatchingStrategy(10, 1000, 1*time.Millisecond),
		NewTimeBasedFlushingStrategy(100*time.Millisecond, 100),
		NewBackpressureStrategy(0.8, 100, 1*time.Millisecond),
	}

	batchPublishStrategies := []BatchPublishStrategy{
		NewSimpleBatchStrategy(1000, 10*time.Millisecond),
		NewAdaptiveBatchPublishStrategy(100, 10000, 10*time.Millisecond),
	}

	bufferSizes := []uint64{1024 * 1024}
	modes := []RingBufferMode{DispatchMode}
	workerCounts := []uint32{1, 2}
	testModes := []TestMode{SinglePublishOnly, BatchPublishOnly, HalfSingleHalfBatch}

	// fmt.Println("Starting test loop")
	for _, mode := range modes {
		for _, batchSize := range bufferSizes {
			for _, singleSize := range bufferSizes {
				for _, producerCount := range workerCounts {
					for _, consumerCount := range workerCounts {
						for _, waitStrategy := range waitStrategies {
							for _, singleStrategy := range singlePublishStrategies {
								for _, batchStrategy := range batchPublishStrategies {
									for _, testMode := range testModes {
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
										runTest(testName, config, 2*time.Second, testMode)
									}
								}
							}
						}
					}
				}
			}
		}
	}
}
