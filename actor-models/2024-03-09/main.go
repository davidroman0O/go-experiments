package main

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

const (
	bufferSize    = 1024 * 1024
	batchSize     = 1024 * 4
	killMessageID = -1
)

type Event struct {
	ID    int64
	Data  interface{}
	Ready uint32 // 0 = not ready, 1 = ready
}

type RingBuffer struct {
	events [bufferSize]Event
	head   int64
	tail   int64
	mask   int64
}

func NewRingBuffer() *RingBuffer {
	return &RingBuffer{
		mask: int64(bufferSize - 1),
	}
}

func (rb *RingBuffer) Publish(events []Event) {
	for _, event := range events {
		nextHead := atomic.AddInt64(&rb.head, 1) - 1
		index := nextHead & rb.mask
		rb.events[index] = event
		atomic.StoreUint32(&rb.events[index].Ready, 1)
	}
}

func (rb *RingBuffer) Consume(workerID int, wg *sync.WaitGroup) {
	defer wg.Done()

	for {
		currentTail := atomic.LoadInt64(&rb.tail)
		if currentTail >= atomic.LoadInt64(&rb.head) {
			time.Sleep(5 * time.Nanosecond) // Briefly sleep to reduce busy-waiting
			runtime.Gosched()
			continue
		}

		index := currentTail & rb.mask
		if atomic.LoadUint32(&rb.events[index].Ready) == 0 {
			time.Sleep(5 * time.Nanosecond) // Briefly sleep to reduce busy-waiting
			runtime.Gosched()
			continue
		}

		event := rb.events[index]
		if event.ID == killMessageID {
			fmt.Printf("Worker %d stopping...\n", workerID)
			return
		}

		// Process event
		// fmt.Printf("Worker %d processed: %v\n", workerID, event.Data)

		atomic.StoreUint32(&rb.events[index].Ready, 0)
		atomic.AddInt64(&rb.tail, 1)
	}
}

var constantCPU = (runtime.NumCPU() * runtime.NumCPU()) - 1
var numWorkers = runtime.NumCPU() * 2 // Number of worker goroutines

func main() {
	// Set GOMAXPROCS to utilize multiple CPU cores
	runtime.GOMAXPROCS(constantCPU)

	rb := NewRingBuffer()
	wg := sync.WaitGroup{}
	wg.Add(numWorkers)

	// Start workers
	for i := 0; i < numWorkers; i++ {
		go rb.Consume(i, &wg)
	}

	start := time.Now()
	// Simulate producing messages in batches
	go func() {
		for i := int64(0); i < 30000000; i += batchSize {
			var batch []Event
			for j := int64(0); j < batchSize; j++ {
				batch = append(batch, Event{ID: i + j, Data: fmt.Sprintf("Message %d", i+j)})
			}
			rb.Publish(batch)
			// time.Sleep(1 * time.Nanosecond) // Simulate time between batches
			runtime.Gosched()
		}
		// Send kill message to all workers to stop gracefully
		for i := 0; i < numWorkers; i++ {
			rb.Publish([]Event{{ID: killMessageID}})
		}
	}()

	wg.Wait()
	fmt.Println("All messages processed.")
	elapsed := time.Since(start)

	fmt.Printf("Processed messages in %s\n", elapsed)
}
