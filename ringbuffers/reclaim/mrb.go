package main

import (
	"context"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
)

type BalancerBuffer[T any] struct {
	buffers        []*RingBuffer[T]
	nextIndex      uint64
	bufferStatuses []BufferStatus
	subscribers    []chan []T
	output         chan []T
}

type BufferStatus struct {
	Delta int64
	Err   error
}

func NewBalancerBuffer[T any](numBuffers int, opts ...Option) *BalancerBuffer[T] {
	buffers := make([]*RingBuffer[T], numBuffers)
	bufferStatuses := make([]BufferStatus, numBuffers)
	for i := range buffers {
		buffers[i] = NewRingBuffer[T](context.Background(), opts...)
	}

	output := make(chan []T)

	bb := &BalancerBuffer[T]{
		buffers:        buffers,
		bufferStatuses: bufferStatuses,
		output:         output,
	}

	go bb.fanInMessages()

	return bb
}

func (bb *BalancerBuffer[T]) Enqueue(ctx context.Context, msgs []T) <-chan error {
	done := make(chan error)
	go func() {
		defer close(done)

		for _, msg := range msgs {
			buffer := bb.selectBuffer()
			if buffer != nil {
				err := <-buffer.Enqueue(ctx, []T{msg})
				if err != nil {
					done <- err
					return
				}
			} else {
				// Handle enqueue failure (e.g., all buffers full)
				done <- fmt.Errorf("all buffers are full")
				return
			}
		}

		done <- nil
	}()

	return done
}

func (bb *BalancerBuffer[T]) EnqueueMany(ctx context.Context, msgs []T) <-chan error {
	done := make(chan error)
	go func() {
		defer close(done)

		for len(msgs) > 0 {
			buffer := bb.selectBuffer()
			if buffer != nil {
				err := <-buffer.Enqueue(ctx, msgs)
				if err != nil {
					done <- err
					return
				} else {
					// Enqueue successful
					msgs = nil
				}
			} else {
				// Handle enqueue failure (e.g., all buffers full)
				done <- fmt.Errorf("all buffers are full")
				return
			}
		}

		done <- nil
	}()

	return done
}

func (bb *BalancerBuffer[T]) selectBuffer() *RingBuffer[T] {
	var minDelta int64 = math.MaxInt64
	var selectedBuffer *RingBuffer[T]
	for i, buffer := range bb.buffers {
		status := bb.bufferStatuses[i]
		delta := status.Delta
		if delta < minDelta && status.Err == nil {
			minDelta = delta
			selectedBuffer = buffer
		}
	}

	if selectedBuffer == nil {
		// If no information is available or all buffers have errors, use round-robin
		index := atomic.AddUint64(&bb.nextIndex, 1) % uint64(len(bb.buffers))
		return bb.buffers[index]
	}

	return selectedBuffer
}

func (bb *BalancerBuffer[T]) Tick(index int, delta int64, err error) {
	bb.bufferStatuses[index].Delta = delta
	bb.bufferStatuses[index].Err = err
}

func (bb *BalancerBuffer[T]) Close() {
	for _, buffer := range bb.buffers {
		buffer.Close()
	}
}

func (bb *BalancerBuffer[T]) Subscribe(subscriber chan []T) {
	bb.subscribers = append(bb.subscribers, subscriber)
	for _, buffer := range bb.buffers {
		buffer.Subscribe(subscriber)
	}
}

func (bb *BalancerBuffer[T]) Unsubscribe(subscriber chan []T) {
	for i, sub := range bb.subscribers {
		if sub == subscriber {
			bb.subscribers = append(bb.subscribers[:i], bb.subscribers[i+1:]...)
			break
		}
	}
	for _, buffer := range bb.buffers {
		buffer.Unsubscribe(subscriber)
	}
}

func (bb *BalancerBuffer[T]) Output() <-chan []T {
	return bb.output
}

func (bb *BalancerBuffer[T]) fanInMessages() {
	var wg sync.WaitGroup
	for _, sub := range bb.subscribers {
		wg.Add(1)
		go func(sub chan []T) {
			defer wg.Done()
			for msgs := range sub {
				bb.output <- msgs
			}
		}(sub)
	}
	wg.Wait()
	close(bb.output)
}
