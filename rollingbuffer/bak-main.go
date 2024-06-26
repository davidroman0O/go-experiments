package main

// import (
// 	"context"
// 	"errors"
// 	"fmt"
// 	"runtime"
// 	"sync"
// 	"sync/atomic"
// 	"time"
// )

// func main() {
// 	numCores := runtime.NumCPU()
// 	runtime.GOMAXPROCS(numCores)

// 	rb := NewRollingBuffer(1000, 100)
// 	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
// 	ctx = context.WithValue(ctx, "startTime", time.Now())
// 	defer cancel()

// 	var counter int32
// 	var wg sync.WaitGroup

// 	tick := time.NewTicker(1 * time.Second)

// 	go func() {
// 		for {
// 			select {
// 			case <-tick.C:
// 				fmt.Println("counter:", atomic.LoadInt32(&counter))
// 			case <-ctx.Done():
// 				return
// 			}
// 		}
// 	}()

// 	// Create producers
// 	for i := 0; i < 5; i++ {
// 		wg.Add(1)
// 		go func() {
// 			defer wg.Done()
// 			producer := rb.NewProducer()
// 			for {
// 				select {
// 				case producer <- "data":
// 					// Successful send
// 					// fmt.Println("send")
// 				case <-ctx.Done():
// 					return
// 				}
// 			}
// 		}()
// 	}

// 	// Create consumers
// 	for i := 0; i < 3; i++ {
// 		wg.Add(1)
// 		go func() {
// 			defer wg.Done()
// 			consumer := rb.NewConsumer()
// 			for range consumer {
// 				atomic.AddInt32(&counter, 1)
// 			}
// 		}()
// 	}

// 	// Wait for timeout
// 	<-ctx.Done()

// 	// Close the RollingBuffer
// 	rb.Close(false)

// 	// Wait for all goroutines to finish
// 	wg.Wait()

// 	// Print metrics
// 	elapsed := time.Since(ctx.Value("startTime").(time.Time))
// 	messagesPerSecond := float64(atomic.LoadInt32(&counter)) / elapsed.Seconds()
// 	fmt.Printf("Elapsed time: %v\n", elapsed)
// 	fmt.Printf("Messages consumed per second: %.2f\n", messagesPerSecond)
// }

// type bufferState int

// const (
// 	stateWriting bufferState = iota
// 	stateReading
// 	stateEmpty
// )

// type internalBuffer struct {
// 	ch    chan interface{}
// 	state bufferState
// }

// type RollingBuffer struct {
// 	buffers            []*internalBuffer
// 	currentWriteIndex  int
// 	currentReadIndex   int
// 	producers          []chan interface{}
// 	consumers          []chan interface{}
// 	internalBufferSize int
// 	consumerBufferSize int
// 	mu                 sync.RWMutex
// 	wg                 sync.WaitGroup
// 	closed             atomic.Bool
// }

// func NewRollingBuffer(internalBufferSize, consumerBufferSize int) *RollingBuffer {
// 	rb := &RollingBuffer{
// 		buffers:            make([]*internalBuffer, 1),
// 		internalBufferSize: internalBufferSize,
// 		consumerBufferSize: consumerBufferSize,
// 	}
// 	rb.buffers[0] = &internalBuffer{
// 		ch:    make(chan interface{}, internalBufferSize),
// 		state: stateWriting,
// 	}
// 	return rb
// }

// func (rb *RollingBuffer) NewProducer() chan<- interface{} {
// 	rb.mu.Lock()
// 	defer rb.mu.Unlock()

// 	ch := make(chan interface{}, rb.internalBufferSize)
// 	rb.producers = append(rb.producers, ch)

// 	rb.wg.Add(1)
// 	go rb.handleProducer(ch)

// 	return ch
// }

// func (rb *RollingBuffer) handleProducer(ch <-chan interface{}) {
// 	defer rb.wg.Done()

// 	for item := range ch {
// 		rb.write(item)
// 	}
// }

// func (rb *RollingBuffer) write(item interface{}) {
// 	rb.mu.Lock()
// 	defer rb.mu.Unlock()

// 	currentBuffer := rb.buffers[rb.currentWriteIndex]
// 	select {
// 	case currentBuffer.ch <- item:
// 		// Item written successfully
// 	default:
// 		// Current buffer is full, create a new one
// 		newBuffer := &internalBuffer{
// 			ch:    make(chan interface{}, rb.internalBufferSize),
// 			state: stateWriting,
// 		}
// 		rb.buffers = append(rb.buffers, newBuffer)
// 		rb.currentWriteIndex = len(rb.buffers) - 1
// 		currentBuffer.state = stateReading
// 		newBuffer.ch <- item
// 	}
// }

// func (rb *RollingBuffer) NewConsumer() <-chan interface{} {
// 	rb.mu.Lock()
// 	defer rb.mu.Unlock()

// 	ch := make(chan interface{}, rb.consumerBufferSize)
// 	rb.consumers = append(rb.consumers, ch) // Store the send-only end

// 	rb.wg.Add(1)
// 	go rb.handleConsumer(ch)

// 	return ch // Return the receive-only end to the caller
// }

// func (rb *RollingBuffer) handleConsumer(ch chan<- interface{}) {
// 	defer rb.wg.Done()

// 	for {
// 		if rb.closed.Load() && rb.isEmpty() {
// 			close(ch)
// 			return
// 		}

// 		item, ok := rb.read()
// 		if !ok {
// 			continue
// 		}

// 		select {
// 		case ch <- item:
// 			// Item sent to consumer
// 		default:
// 			// Consumer buffer is full, put the item back
// 			rb.write(item)
// 		}
// 	}
// }

// func (rb *RollingBuffer) read() (interface{}, bool) {
// 	rb.mu.Lock()
// 	defer rb.mu.Unlock()

// 	if len(rb.buffers) == 0 {
// 		return nil, false
// 	}

// 	currentBuffer := rb.buffers[rb.currentReadIndex]
// 	select {
// 	case item := <-currentBuffer.ch:
// 		if len(currentBuffer.ch) == 0 {
// 			currentBuffer.state = stateEmpty
// 			rb.currentReadIndex = (rb.currentReadIndex + 1) % len(rb.buffers)
// 		}
// 		return item, true
// 	default:
// 		rb.currentReadIndex = (rb.currentReadIndex + 1) % len(rb.buffers)
// 		return nil, false
// 	}
// }

// func (rb *RollingBuffer) isEmpty() bool {
// 	rb.mu.RLock()
// 	defer rb.mu.RUnlock()

// 	for _, buf := range rb.buffers {
// 		if len(buf.ch) > 0 {
// 			return false
// 		}
// 	}
// 	return true
// }

// func (rb *RollingBuffer) Close(waitForConsumers bool) error {
// 	if rb.closed.Swap(true) {
// 		return errors.New("RollingBuffer already closed")
// 	}

// 	// Close all producer channels
// 	rb.mu.Lock()
// 	for _, ch := range rb.producers {
// 		close(ch)
// 	}
// 	rb.mu.Unlock()

// 	// Wait for all producers to finish
// 	rb.wg.Wait()

// 	if waitForConsumers {
// 		// Wait for all buffers to be emptied
// 		for !rb.isEmpty() {
// 			// Sleep or yield to allow consumers to process
// 		}
// 	}

// 	// Close all consumer channels
// 	rb.mu.Lock()
// 	for _, ch := range rb.consumers {
// 		close(ch)
// 	}
// 	rb.mu.Unlock()

// 	return nil
// }
