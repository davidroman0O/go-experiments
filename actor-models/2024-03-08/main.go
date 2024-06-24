package main

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

/// This is by far the best one i got, i need to refactor that one to be used on gronos

// Worker function adjusted to use notifier for pausing and resuming
func Worker(id int, cb func(nb int32), wg *sync.WaitGroup, rb *RingBuffer, quit chan struct{}) {
	defer wg.Done()
	for {
		msgs, ok := rb.dequeueMany(messageBatchSize)
		if !ok {
			select {
			case <-rb.notifier: // Wait for a signal that new messages are available
				// fmt.Println("pause")
				runtime.Gosched()
				continue
			case <-quit: // Check if we're quitting
				fmt.Println("quit", id)
				return
			}
		}
		for _, msg := range msgs {
			if msg.Kill {
				// Received a kill message, exit gracefully
				return
			}
			// Simulate some work on the message
			_ = msg.Value * 2
		}
		cb(int32(len(msgs)))
	}
}

var constantCPU = (runtime.NumCPU() * runtime.NumCPU())

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
	// Set GOMAXPROCS to utilize multiple CPU cores
	runtime.GOMAXPROCS(constantCPU)

	// Create a quit channel to signal workers to stop
	quit := make(chan struct{})

	// Create a MultiRingBuffer for message passing
	mrb := newRingBuffer(ringBufferSize, quit)

	var total uint32 = 0

	cb := func(nb int32) {
		atomic.AddUint32(&total, uint32(nb))
	}

	// Create workers for each RingBuffer in MultiRingBuffer
	var wg sync.WaitGroup
	wg.Add(numWorkers)
	for i := 0; i < numWorkers; i++ {
		go Worker(i, cb, &wg, mrb, quit)
	}

	// Generate and enqueue messages
	start := time.Now()
	produced := 0
	for i := 0; i < numMessages; i += messageBatchSize {
		batch := make([]Message, min(messageBatchSize, numMessages-i))
		for j := 0; j < len(batch); j++ {
			batch[j] = Message{Value: i + j}
			mrb.enqueue(batch[j])
		}
		// for !mrb.enqueueMany(batch) {
		// 	// MultiRingBuffer is full, wait for workers to catch up
		// 	time.Sleep(10 * time.Nanosecond)
		// }
		produced += len(batch)
	}

	// Send a kill message to each worker by enqueueing it directly to each buffer
	// for _, buffer := range mrb.buffers {
	killMsg := Message{Kill: true}
	for !mrb.enqueue(killMsg) {
		// If a buffer is full, wait a bit and try again
		time.Sleep(10 * time.Nanosecond)
	}
	// }

	// Wait for all workers to finish
	wg.Wait()

	// Close the quit channel to signal workers to stop
	close(quit)

	elapsed := time.Since(start)

	fmt.Printf("Processed %d messages in %s - produced %d - total processed: %v \n", numMessages, elapsed, produced, total)
}

// Utility function to calculate the minimum of two integers
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
