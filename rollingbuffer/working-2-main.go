package main

// import (
// 	"context"
// 	"fmt"
// 	"runtime"
// 	"sync"
// 	"sync/atomic"
// 	"time"
// )

// type ChannelState int32

// const (
// 	ChannelOpen ChannelState = iota
// 	ChannelClosed
// )

// type ProducerChannel struct {
// 	ch    chan<- interface{}
// 	state *atomic.Int32
// }

// type ConsumerChannel struct {
// 	ch     chan interface{}
// 	closed chan struct{}
// }

// type RingBuffer struct {
// 	buffer []interface{}
// 	head   int
// 	tail   int
// 	count  int
// 	size   int
// 	mu     sync.Mutex
// }

// type RollingBuffer struct {
// 	producerChannels []ProducerChannel
// 	consumerChannels []ConsumerChannel
// 	ringBuffers      []*RingBuffer
// 	currentBuffer    *RingBuffer
// 	bufferSize       int
// 	consumerSize     int
// 	mu               sync.Mutex
// }

// func NewRollingBuffer(bufferSize, consumerSize int) *RollingBuffer {
// 	rb := &RollingBuffer{
// 		bufferSize:   bufferSize,
// 		consumerSize: consumerSize,
// 	}
// 	rb.currentBuffer = NewRingBuffer(bufferSize)
// 	rb.ringBuffers = append(rb.ringBuffers, rb.currentBuffer)
// 	return rb
// }

// func NewRingBuffer(size int) *RingBuffer {
// 	return &RingBuffer{
// 		buffer: make([]interface{}, size),
// 		size:   size,
// 	}
// }

// func (rb *RollingBuffer) NewProducer() ProducerChannel {
// 	ch := make(chan interface{}, rb.bufferSize)
// 	pc := ProducerChannel{
// 		ch:    ch,
// 		state: &atomic.Int32{},
// 	}
// 	rb.mu.Lock()
// 	rb.producerChannels = append(rb.producerChannels, pc)
// 	rb.mu.Unlock()
// 	go rb.producerWorker(ch)
// 	return pc
// }

// func (rb *RollingBuffer) NewConsumer() ConsumerChannel {
// 	ch := make(chan interface{}, rb.consumerSize)
// 	closed := make(chan struct{})
// 	cc := ConsumerChannel{ch: ch, closed: closed}
// 	rb.mu.Lock()
// 	rb.consumerChannels = append(rb.consumerChannels, cc)
// 	rb.mu.Unlock()
// 	go rb.consumerWorker(ch, closed)
// 	return cc
// }

// func (rb *RollingBuffer) producerWorker(ch <-chan interface{}) {
// 	for data := range ch {
// 		rb.mu.Lock()
// 		if rb.currentBuffer.IsFull() {
// 			rb.currentBuffer = NewRingBuffer(rb.bufferSize)
// 			rb.ringBuffers = append(rb.ringBuffers, rb.currentBuffer)
// 		}
// 		rb.currentBuffer.Write(data)
// 		rb.mu.Unlock()
// 	}
// }

// func (rb *RollingBuffer) consumerWorker(ch chan interface{}, closed chan struct{}) {
// 	for {
// 		select {
// 		case <-closed:
// 			close(ch)
// 			return
// 		default:
// 			rb.mu.Lock()
// 			data, ok := rb.read()
// 			rb.mu.Unlock()
// 			if !ok {
// 				time.Sleep(10 * time.Millisecond)
// 				continue
// 			}
// 			select {
// 			case ch <- data:
// 			case <-closed:
// 				close(ch)
// 				return
// 			}
// 		}
// 	}
// }
// func (rb *RollingBuffer) read() (interface{}, bool) {
// 	for i, buffer := range rb.ringBuffers {
// 		if !buffer.IsEmpty() {
// 			data := buffer.Read()
// 			if buffer.IsEmpty() {
// 				rb.ringBuffers = append(rb.ringBuffers[:i], rb.ringBuffers[i+1:]...)
// 			}
// 			return data, true
// 		}
// 	}
// 	return nil, false
// }

// func (rb *RollingBuffer) Close(wait bool) {
// 	fmt.Println("Starting Close function...")
// 	rb.mu.Lock()

// 	fmt.Println("Closing producer channels...")
// 	for i, pc := range rb.producerChannels {
// 		pc.state.Store(int32(ChannelClosed))
// 		close(pc.ch)
// 		fmt.Printf("Closed producer channel %d\n", i)
// 	}
// 	rb.mu.Unlock()

// 	if wait {
// 		fmt.Println("Waiting for consumers to drain queues...")
// 		ticker := time.NewTicker(1 * time.Second / 2)
// 		go func() {
// 			for {
// 				<-ticker.C
// 				fmt.Printf("Remaining buffers: %d\n", len(rb.ringBuffers))
// 			}
// 		}()
// 		for {
// 			rb.mu.Lock()
// 			remainingBuffers := len(rb.ringBuffers)
// 			rb.mu.Unlock()
// 			if remainingBuffers == 0 {
// 				break
// 			}
// 			// fmt.Printf("Remaining buffers: %d\n", remainingBuffers)
// 			// time.Sleep(1 * time.Microsecond)
// 		}
// 		ticker.Stop()
// 	}

// 	rb.mu.Lock()
// 	fmt.Println("Closing consumer channels...")
// 	for i, cc := range rb.consumerChannels {
// 		close(cc.closed)
// 		fmt.Printf("Closed consumer channel %d\n", i)
// 	}

// 	rb.producerChannels = nil
// 	rb.consumerChannels = nil
// 	rb.ringBuffers = nil
// 	rb.currentBuffer = nil
// 	rb.mu.Unlock()

// 	fmt.Println("RollingBuffer closed.")
// }

// func (pc ProducerChannel) Write(data interface{}) error {
// 	if pc.state.Load() == int32(ChannelClosed) {
// 		return fmt.Errorf("channel is closed")
// 	}
// 	select {
// 	case pc.ch <- data:
// 		return nil
// 	default:
// 		return fmt.Errorf("channel is full")
// 	}
// }

// func (rb *RingBuffer) Write(data interface{}) {
// 	rb.mu.Lock()
// 	defer rb.mu.Unlock()
// 	rb.buffer[rb.tail] = data
// 	rb.tail = (rb.tail + 1) % rb.size
// 	rb.count++
// }

// func (rb *RingBuffer) Read() interface{} {
// 	rb.mu.Lock()
// 	defer rb.mu.Unlock()
// 	data := rb.buffer[rb.head]
// 	rb.head = (rb.head + 1) % rb.size
// 	rb.count--
// 	return data
// }

// func (rb *RingBuffer) IsFull() bool {
// 	return rb.count == rb.size
// }

// func (rb *RingBuffer) IsEmpty() bool {
// 	return rb.count == 0
// }

// func main() {
// 	numCores := runtime.NumCPU()
// 	runtime.GOMAXPROCS(numCores)
// 	bufferSize := 1024 * 16
// 	consumerSize := 1024 * 8
// 	numProducers := 5 * 10
// 	numConsumers := 3 * 10

// 	rb := NewRollingBuffer(bufferSize, consumerSize)
// 	now := time.Now()
// 	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
// 	defer cancel()

// 	var counter int32
// 	var wg sync.WaitGroup

// 	fmt.Println("Starting producers and consumers...")

// 	for i := 0; i < numProducers; i++ {
// 		pc := rb.NewProducer()
// 		wg.Add(1)
// 		go func(id int) {
// 			defer wg.Done()
// 			for {
// 				select {
// 				case <-ctx.Done():
// 					fmt.Printf("Producer %d stopping\n", id)
// 					return
// 				default:
// 					err := pc.Write(1)
// 					if err != nil {
// 						if err.Error() == "channel is closed" {
// 							fmt.Printf("Producer %d channel closed\n", id)
// 							return
// 						}
// 						time.Sleep(time.Millisecond)
// 					}
// 				}
// 			}
// 		}(i)
// 	}

// 	for i := 0; i < numConsumers; i++ {
// 		cc := rb.NewConsumer()
// 		wg.Add(1)
// 		go func(id int) {
// 			defer wg.Done()
// 			for {
// 				_, ok := <-cc.ch
// 				if !ok {
// 					fmt.Printf("Consumer %d channel closed\n", id)
// 					return
// 				}
// 				atomic.AddInt32(&counter, 1)
// 			}
// 		}(i)
// 	}

// 	fmt.Println("Waiting for timeout...")
// 	<-ctx.Done()
// 	fmt.Println("Timeout reached. Closing RollingBuffer...")

// 	rb.Close(true)

// 	fmt.Println("Waiting for all goroutines to finish...")
// 	wg.Wait()

// 	elapsed := time.Since(now)
// 	messagesPerSecond := float64(counter) / elapsed.Seconds()

// 	fmt.Printf("Elapsed time: %v\n", elapsed)
// 	fmt.Printf("Total messages processed: %d\n", counter)
// 	fmt.Printf("Messages processed per second: %.2f\n", messagesPerSecond)
// }
