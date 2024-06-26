package main

// import (
// 	"context"
// 	"fmt"
// 	"sync"
// 	"sync/atomic"
// 	"time"
// )

// type RollingBuffer struct {
// 	producers     []chan interface{}
// 	consumers     []chan interface{}
// 	internalBuffs []*ringBuffer
// 	currentBuff   *ringBuffer
// 	producerSize  int
// 	consumerSize  int
// 	mu            sync.Mutex
// }

// type ringBuffer struct {
// 	buffer []interface{}
// 	read   int
// 	write  int
// 	full   bool
// }

// func NewRollingBuffer(internalSize, consumerSize int) *RollingBuffer {
// 	rb := &RollingBuffer{
// 		producerSize:  internalSize,
// 		consumerSize:  consumerSize,
// 		internalBuffs: make([]*ringBuffer, 1),
// 	}
// 	rb.internalBuffs[0] = &ringBuffer{buffer: make([]interface{}, internalSize)}
// 	rb.currentBuff = rb.internalBuffs[0]
// 	return rb
// }

// func (rb *RollingBuffer) NewProducer() chan<- interface{} {
// 	ch := make(chan interface{}, rb.producerSize)
// 	rb.producers = append(rb.producers, ch)
// 	go rb.readFromProducer(ch)
// 	return ch
// }

// func (rb *RollingBuffer) NewConsumer() <-chan interface{} {
// 	ch := make(chan interface{}, rb.consumerSize)
// 	rb.consumers = append(rb.consumers, ch)
// 	go rb.writeToConsumer(ch)
// 	return ch
// }

// func (rb *RollingBuffer) readFromProducer(ch <-chan interface{}) {
// 	for data := range ch {
// 		rb.mu.Lock()
// 		if rb.currentBuff.full {
// 			newBuff := &ringBuffer{buffer: make([]interface{}, rb.producerSize)}
// 			rb.internalBuffs = append(rb.internalBuffs, newBuff)
// 			rb.currentBuff = newBuff
// 		}
// 		rb.currentBuff.buffer[rb.currentBuff.write] = data
// 		rb.currentBuff.write = (rb.currentBuff.write + 1) % len(rb.currentBuff.buffer)
// 		if rb.currentBuff.write == rb.currentBuff.read {
// 			rb.currentBuff.full = true
// 		}
// 		rb.mu.Unlock()
// 	}
// }

// func (rb *RollingBuffer) writeToConsumer(ch chan<- interface{}) {
// 	for {
// 		rb.mu.Lock()
// 		if len(rb.internalBuffs) == 0 {
// 			rb.mu.Unlock()
// 			return
// 		}
// 		buff := rb.internalBuffs[0]
// 		if buff.read == buff.write && !buff.full {
// 			if len(rb.internalBuffs) > 1 {
// 				rb.internalBuffs = rb.internalBuffs[1:]
// 				buff = rb.internalBuffs[0]
// 			} else {
// 				rb.mu.Unlock()
// 				time.Sleep(time.Millisecond)
// 				continue
// 			}
// 		}
// 		data := buff.buffer[buff.read]
// 		buff.read = (buff.read + 1) % len(buff.buffer)
// 		buff.full = false
// 		rb.mu.Unlock()
// 		ch <- data
// 	}
// }

// func (rb *RollingBuffer) Close(wait bool) {
// 	for _, ch := range rb.producers {
// 		close(ch)
// 	}
// 	if wait {
// 		for len(rb.internalBuffs) > 0 || len(rb.producers) > 0 {
// 			time.Sleep(time.Millisecond)
// 		}
// 	}
// 	for _, ch := range rb.consumers {
// 		close(ch)
// 	}
// }

// func main() {
// 	rb := NewRollingBuffer(1000, 100)
// 	now := time.Now()
// 	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
// 	defer cancel()

// 	var counter int32

// 	// Producers
// 	for i := 0; i < 5; i++ {
// 		go func() {
// 			ch := rb.NewProducer()
// 			for {
// 				select {
// 				case <-ctx.Done():
// 					return
// 				default:
// 					ch <- struct{}{}
// 				}
// 			}
// 		}()
// 	}

// 	// Consumers
// 	for i := 0; i < 3; i++ {
// 		go func() {
// 			ch := rb.NewConsumer()
// 			for range ch {
// 				atomic.AddInt32(&counter, 1)
// 			}
// 		}()
// 	}

// 	<-ctx.Done()
// 	rb.Close(true)

// 	elapsed := time.Since(now)
// 	fmt.Printf("Elapsed time: %v\n", elapsed)
// 	fmt.Printf("Messages per second: %.2f\n", float64(counter)/elapsed.Seconds())
// }
