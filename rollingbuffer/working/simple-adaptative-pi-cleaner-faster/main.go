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

type PIDController struct {
	kp, ki, kd float64
	setpoint   float64
	integral   float64
	lastError  float64
	lastOutput float64
}

func NewPIDController(kp, ki, kd, setpoint float64) *PIDController {
	return &PIDController{kp: kp, ki: ki, kd: kd, setpoint: setpoint}
}

func (pid *PIDController) Compute(input float64, dt float64) float64 {
	error := pid.setpoint - input
	pid.integral += error * dt
	derivative := (error - pid.lastError) / dt
	output := pid.kp*error + pid.ki*pid.integral + pid.kd*derivative
	pid.lastError = error
	pid.lastOutput = output
	return output
}

type WorkerAdjustment struct {
	ProducerAdjustment int
	ConsumerAdjustment int
}

type AdaptiveWorkerManager struct {
	producerCount     atomic.Int32
	consumerCount     atomic.Int32
	deltas            []float64
	lastAdjustment    time.Time
	pid               *PIDController
	rateLimit         time.Duration
	adjustmentChannel chan WorkerAdjustment
}

func NewAdaptiveWorkerManager(adjustmentChannel chan WorkerAdjustment) *AdaptiveWorkerManager {
	return &AdaptiveWorkerManager{
		deltas:            make([]float64, 0, 10),
		lastAdjustment:    time.Now(),
		pid:               NewPIDController(0.5, 0.2, 0.1, 0),
		rateLimit:         time.Second * 5,
		adjustmentChannel: adjustmentChannel,
	}
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

type Message struct {
	Data interface{}
}

type RollingBuffer struct {
	producerChannels  []chan Message
	consumerChannels  []chan Message
	workerManager     *AdaptiveWorkerManager
	ctx               context.Context
	cancel            context.CancelFunc
	producedCount     atomic.Int32
	consumedCount     atomic.Int32
	adjustmentChannel chan WorkerAdjustment
}

func NewRollingBuffer(bufferSize int, adjustmentChannel chan WorkerAdjustment) *RollingBuffer {
	ctx, cancel := context.WithCancel(context.Background())
	rb := &RollingBuffer{
		adjustmentChannel: adjustmentChannel,
		ctx:               ctx,
		cancel:            cancel,
	}
	rb.workerManager = NewAdaptiveWorkerManager(adjustmentChannel)
	go rb.manageWorkers()
	return rb
}

func (rb *RollingBuffer) NewProducer(initialWorkers int) chan<- Message {
	ch := make(chan Message, 1000)
	rb.producerChannels = append(rb.producerChannels, ch)
	for i := 0; i < initialWorkers; i++ {
		go rb.producerWorker(ch)
	}
	rb.workerManager.producerCount.Add(int32(initialWorkers))
	return ch
}

func (rb *RollingBuffer) NewConsumer(initialWorkers int) <-chan Message {
	ch := make(chan Message, 1000)
	rb.consumerChannels = append(rb.consumerChannels, ch)
	for i := 0; i < initialWorkers; i++ {
		go rb.consumerWorker(ch)
	}
	rb.workerManager.consumerCount.Add(int32(initialWorkers))
	return ch
}

func (rb *RollingBuffer) producerWorker(ch <-chan Message) {
	for {
		select {
		case <-rb.ctx.Done():
			return
		case msg := <-ch:
			rb.producedCount.Add(1)
			rb.distributeMessage(msg)
		}
	}
}

func (rb *RollingBuffer) consumerWorker(ch chan<- Message) {
	for {
		select {
		case <-rb.ctx.Done():
			return
		case msg := <-rb.getNextMessage():
			ch <- msg
			rb.consumedCount.Add(1)
		}
	}
}

func (rb *RollingBuffer) distributeMessage(msg Message) {
	for _, ch := range rb.consumerChannels {
		select {
		case ch <- msg:
			return
		default:
			// Channel is full, try the next one
		}
	}
	// If all channels are full, create a new one
	newCh := make(chan Message, 1000)
	rb.consumerChannels = append(rb.consumerChannels, newCh)
	go rb.consumerWorker(newCh)
	newCh <- msg
}

func (rb *RollingBuffer) getNextMessage() <-chan Message {
	return rb.consumerChannels[0] // Simplified for this example
}

func (rb *RollingBuffer) manageWorkers() {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			producedCount := rb.producedCount.Load()
			consumedCount := rb.consumedCount.Load()
			delta := producedCount - consumedCount
			avgConsumptionRate := float64(consumedCount) / 0.1 // Simplified calculation
			rb.workerManager.AdjustWorkers(delta, avgConsumptionRate)
		case adjustment := <-rb.adjustmentChannel:
			rb.applyWorkerAdjustment(adjustment)
		case <-rb.ctx.Done():
			return
		}
	}
}

func (rb *RollingBuffer) applyWorkerAdjustment(adjustment WorkerAdjustment) {
	if adjustment.ConsumerAdjustment > 0 {
		for i := 0; i < adjustment.ConsumerAdjustment; i++ {
			go rb.consumerWorker(rb.consumerChannels[0])
			rb.workerManager.consumerCount.Add(1)
		}
	} else if adjustment.ConsumerAdjustment < 0 {
		rb.workerManager.consumerCount.Add(int32(adjustment.ConsumerAdjustment))
	}

	if adjustment.ProducerAdjustment > 0 {
		for i := 0; i < adjustment.ProducerAdjustment; i++ {
			go rb.producerWorker(rb.producerChannels[0])
			rb.workerManager.producerCount.Add(1)
		}
	} else if adjustment.ProducerAdjustment < 0 {
		rb.workerManager.producerCount.Add(int32(adjustment.ProducerAdjustment))
	}
}

func (rb *RollingBuffer) Close() {
	rb.cancel()
	// Close all channels and perform cleanup
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	adjustmentChannel := make(chan WorkerAdjustment, 100)
	rb := NewRollingBuffer(1024*8, adjustmentChannel)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	producerCh := rb.NewProducer(2)
	consumerCh := rb.NewConsumer(2)

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			default:
				producerCh <- Message{Data: 1}
				time.Sleep(time.Microsecond)
			}
		}
	}()

	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case <-consumerCh:
				// Process message
			}
		}
	}()

	wg.Wait()
	rb.Close()

	fmt.Printf("Total messages produced: %d\n", rb.producedCount.Load())
	fmt.Printf("Total messages consumed: %d\n", rb.consumedCount.Load())
}
