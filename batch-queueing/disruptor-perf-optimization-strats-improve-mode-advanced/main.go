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

func (s *Sequence) IncrementAndGet() int64 {
	return s.value.Add(1) - 1
}

type Event struct {
	value int64
}

type DisruptorInfo interface {
	GetSingleBufferSize() int64
	GetBatchBufferSize() int64
	GetSingleCurrentSequence() int64
	GetBatchCurrentSequence() int64
	GetConsumerSequences() []int64
	GetMode() RingBufferMode
}

type WaitStrategy interface {
	Wait(sequence int64, cursor *Sequence, dependentSequence *Sequence, barrier Sequencer) int64
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
	Next(n int64) int64
	Publish(lo, hi int64)
	IsAvailable(sequence int64) bool
	GetHighestPublishedSequence(nextSequence, availableSequence int64) int64
}

type SingleRingBuffer struct {
	buffer    []Event
	mask      int64
	sequence  *Sequence
	_padding0 padding
}

func NewSingleRingBuffer(size int64) *SingleRingBuffer {
	return &SingleRingBuffer{
		buffer:   make([]Event, size),
		mask:     size - 1,
		sequence: NewSequence(-1),
	}
}

type BatchRingBuffer struct {
	buffer    []Event
	mask      int64
	sequence  *Sequence
	_padding0 padding
}

func NewBatchRingBuffer(size int64) *BatchRingBuffer {
	return &BatchRingBuffer{
		buffer:   make([]Event, size),
		mask:     size - 1,
		sequence: NewSequence(-1),
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

func (d *Disruptor) GetSingleBufferSize() int64      { return int64(len(d.singleRingBuffer.buffer)) }
func (d *Disruptor) GetBatchBufferSize() int64       { return int64(len(d.batchRingBuffer.buffer)) }
func (d *Disruptor) GetSingleCurrentSequence() int64 { return d.singleRingBuffer.sequence.Get() }
func (d *Disruptor) GetBatchCurrentSequence() int64  { return d.batchRingBuffer.sequence.Get() }
func (d *Disruptor) GetConsumerSequences() []int64 {
	sequences := make([]int64, len(d.consumers))
	for i, consumer := range d.consumers {
		sequences[i] = consumer.sequence.Get()
	}
	return sequences
}
func (d *Disruptor) GetMode() RingBufferMode { return d.mode }

type DisruptorConfig struct {
	BatchBufferSize  int64
	SingleBufferSize int64
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

	if err := d.singleStrategy.Initialize(d); err != nil {
		return nil, fmt.Errorf("failed to initialize single publish strategy: %w", err)
	}
	if err := d.batchStrategy.Initialize(d); err != nil {
		return nil, fmt.Errorf("failed to initialize batch publish strategy: %w", err)
	}

	if config.ConsumerCount < 1 {
		return nil, fmt.Errorf("at least one consumer is required")
	}

	for i := 0; i < config.ConsumerCount; i++ {
		d.consumers = append(d.consumers, NewConsumer(d, i))
	}

	for i := 0; i < config.ProducerCount; i++ {
		d.producers = append(d.producers, NewProducer(d))
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

func (p *Producer) PublishSingle(value int64) bool {
	if p.disruptor.singleStrategy.ShouldApplyBackpressure() {
		return false
	}
	sequence := p.disruptor.singleRingBuffer.sequence.IncrementAndGet()
	event := &p.disruptor.singleRingBuffer.buffer[sequence&p.disruptor.singleRingBuffer.mask]
	event.value = value
	return true
}

func (p *Producer) PublishBatch(batch []int64) bool {
	optimalSize := p.disruptor.batchStrategy.GetOptimalPublishBatchSize()
	if len(batch) > optimalSize {
		batch = batch[:optimalSize]
	}
	n := int64(len(batch))
	sequence := p.disruptor.batchRingBuffer.sequence.IncrementAndGet()
	for i, value := range batch {
		event := &p.disruptor.batchRingBuffer.buffer[(sequence+int64(i))&p.disruptor.batchRingBuffer.mask]
		event.value = value
	}
	p.disruptor.batchRingBuffer.sequence.Set(sequence + n - 1)
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
		sequence:  NewSequence(-1),
	}
}

func (c *Consumer) processBatchesDispatch() {
	defer c.disruptor.wg.Done()
	for !c.disruptor.consumersShutdown.Load() {
		nextSequence := c.sequence.Get() + 1
		availableSequence := c.disruptor.batchRingBuffer.sequence.Get()

		for nextSequence <= availableSequence {
			if nextSequence%int64(len(c.disruptor.consumers)) == int64(c.id) {
				event := &c.disruptor.batchRingBuffer.buffer[nextSequence&c.disruptor.batchRingBuffer.mask]
				// Process event
				_ = event.value
			}
			nextSequence++
		}

		c.sequence.Set(availableSequence)
	}
}

func (c *Consumer) processBatchesBroadcast() {
	defer c.disruptor.wg.Done()
	for !c.disruptor.consumersShutdown.Load() {
		nextSequence := c.sequence.Get() + 1
		availableSequence := c.disruptor.batchRingBuffer.sequence.Get()

		for nextSequence <= availableSequence {
			event := &c.disruptor.batchRingBuffer.buffer[nextSequence&c.disruptor.batchRingBuffer.mask]
			// Process event
			_ = event.value
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
			if nextSequence%int64(len(c.disruptor.consumers)) == int64(c.id) {
				event := &c.disruptor.singleRingBuffer.buffer[nextSequence&c.disruptor.singleRingBuffer.mask]
				// Process event
				_ = event.value
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
			// Process event
			_ = event.value
			nextSequence++
		}

		c.sequence.Set(nextSequence - 1)
	}
}

// Wait Strategies

type BusySpinWaitStrategy struct{}

func (BusySpinWaitStrategy) Wait(sequence int64, cursor *Sequence, dependentSequence *Sequence, barrier Sequencer) int64 {
	for {
		if cursorValue := cursor.Get(); cursorValue >= sequence {
			return cursorValue
		}
		runtime.Gosched()
	}
}

func (BusySpinWaitStrategy) SignalAllWhenBlocking() {}

type YieldingWaitStrategy struct {
	spinTries int32
}

func NewYieldingWaitStrategy() *YieldingWaitStrategy {
	return &YieldingWaitStrategy{spinTries: 100}
}

func (y *YieldingWaitStrategy) Wait(sequence int64, cursor *Sequence, dependentSequence *Sequence, barrier Sequencer) int64 {
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

func (s *SleepingWaitStrategy) Wait(sequence int64, cursor *Sequence, dependentSequence *Sequence, barrier Sequencer) int64 {
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

func (b *BlockingWaitStrategy) Wait(sequence int64, cursor *Sequence, dependentSequence *Sequence, barrier Sequencer) int64 {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	for {
		if cursorValue := cursor.Get(); cursorValue >= sequence {
			return cursorValue
		}
		b.cond.Wait()
	}
}

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
	currentBatchSize int
	minBatchSize     int
	maxBatchSize     int
}

func NewAdaptiveBatchingStrategy(minBatchSize, maxBatchSize int) *AdaptiveBatchingStrategy {
	return &AdaptiveBatchingStrategy{
		currentBatchSize: minBatchSize,
		minBatchSize:     minBatchSize,
		maxBatchSize:     maxBatchSize,
	}
}

func (s *AdaptiveBatchingStrategy) Initialize(d DisruptorInfo) error {
	if len(d.GetConsumerSequences()) == 0 {
		return fmt.Errorf("adaptive batching strategy requires at least one consumer")
	}
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
	if occupancy > 0.8 {
		s.currentBatchSize = int(math.Min(float64(s.currentBatchSize*2), float64(s.maxBatchSize)))
	} else if occupancy < 0.2 {
		s.currentBatchSize = int(math.Max(float64(s.currentBatchSize/2), float64(s.minBatchSize)))
	}
}

func (s *AdaptiveBatchingStrategy) GetOptimalConsumeBatchSize() int {
	return s.currentBatchSize
}

func (s *AdaptiveBatchingStrategy) ShouldApplyBackpressure() bool {
	return s.currentBatchSize == s.maxBatchSize
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

func (s *TimeBasedFlushingStrategy) GetOptimalConsumeBatchSize() int {
	return s.batchSize
}

func (s *TimeBasedFlushingStrategy) ShouldApplyBackpressure() bool {
	return false
}

type BackpressureStrategy struct {
	threshold     float64
	currentBuffer DisruptorInfo
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
	s.currentBuffer = d
	return nil
}

func (s *BackpressureStrategy) Update(d DisruptorInfo) {
	s.currentBuffer = d
}

func (s *BackpressureStrategy) GetOptimalConsumeBatchSize() int {
	return 100 // Example fixed batch size
}

func (s *BackpressureStrategy) ShouldApplyBackpressure() bool {
	if s.currentBuffer == nil {
		return false
	}
	sequences := s.currentBuffer.GetConsumerSequences()
	if len(sequences) == 0 {
		return false // No consumers, so no backpressure
	}
	minConsumerSequence := sequences[0]
	for _, seq := range sequences[1:] {
		if seq < minConsumerSequence {
			minConsumerSequence = seq
		}
	}
	occupancy := float64(s.currentBuffer.GetSingleCurrentSequence()-minConsumerSequence) / float64(s.currentBuffer.GetSingleBufferSize())
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
func (s *SimpleBatchStrategy) GetOptimalPublishBatchSize() int  { return s.batchSize }
func (s *SimpleBatchStrategy) GetMaxWaitTime() time.Duration    { return s.maxWaitTime }

type AdaptiveBatchPublishStrategy struct {
	currentBatchSize int
	minBatchSize     int
	maxBatchSize     int
	maxWaitTime      time.Duration
}

func NewAdaptiveBatchPublishStrategy(minBatchSize, maxBatchSize int, maxWaitTime time.Duration) *AdaptiveBatchPublishStrategy {
	return &AdaptiveBatchPublishStrategy{
		currentBatchSize: minBatchSize,
		minBatchSize:     minBatchSize,
		maxBatchSize:     maxBatchSize,
		maxWaitTime:      maxWaitTime,
	}
}

func (s *AdaptiveBatchPublishStrategy) Initialize(d DisruptorInfo) error {
	if len(d.GetConsumerSequences()) == 0 {
		return fmt.Errorf("adaptive batching strategy requires at least one consumer")
	}
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
	if occupancy > 0.8 {
		s.currentBatchSize = int(math.Min(float64(s.currentBatchSize*2), float64(s.maxBatchSize)))
	} else if occupancy < 0.2 {
		s.currentBatchSize = int(math.Max(float64(s.currentBatchSize/2), float64(s.minBatchSize)))
	}
}

func (s *AdaptiveBatchPublishStrategy) GetOptimalPublishBatchSize() int {
	return s.currentBatchSize
}

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

	var totalProduced, totalConsumed atomic.Int64

	// Start producers
	for _, p := range disruptor.producers {
		go func(prod *Producer) {
			for {
				select {
				case <-ctx.Done():
					return
				default:
					if rand.Float32() < 0.5 {
						if prod.PublishSingle(rand.Int63()) {
							totalProduced.Add(1)
						}
					} else {
						batch := make([]int64, rand.Intn(producerBatchSize)+1)
						for i := range batch {
							batch[i] = rand.Int63()
						}
						if prod.PublishBatch(batch) {
							totalProduced.Add(int64(len(batch)))
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
				var consumed int64
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

	bufferSizes := []int64{1024 * 1024 * 64, 1024, 4096, 16384, 65536}
	modes := []RingBufferMode{DispatchMode, BroadcastMode}
	workerCounts := []int{1, 2, 4, 8}

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
									runTest(testName, config, 5*time.Second)
								}
							}
						}
					}
				}
			}
		}
	}
}
