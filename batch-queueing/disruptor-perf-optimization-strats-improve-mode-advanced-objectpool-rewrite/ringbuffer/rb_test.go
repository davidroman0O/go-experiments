package ringbuffer

import (
	"fmt"
	"math/rand"
	"reflect"
	"sync/atomic"
	"testing"
	"time"
)

type TestItem struct {
	Value int
}

func TestSinglePublisherRingBufferThroughput(t *testing.T) {
	bufferSize := uint64(1024 * 1024)
	batchSize := uint64(1000)
	flushInterval := 100 * time.Millisecond
	numWorkers := 4
	duration := 5 * time.Second

	var itemsProcessed int64

	handler := func(items []interface{}) {
		atomic.AddInt64(&itemsProcessed, int64(len(items)))
	}

	rb := NewSinglePublisherRingBuffer(bufferSize, reflect.TypeOf(TestItem{}), batchSize, flushInterval, numWorkers, handler)

	start := time.Now()
	end := start.Add(duration)

	var itemsPublished int64
	for time.Now().Before(end) {
		obj := rb.GetObject().(*TestItem)
		obj.Value = rand.Int()
		if rb.TryPublish(obj) {
			atomic.AddInt64(&itemsPublished, 1)
		}
	}

	rb.Shutdown()
	rb.Wait()

	elapsedSeconds := time.Since(start).Seconds()
	throughputPublished := float64(itemsPublished) / elapsedSeconds
	throughputProcessed := float64(atomic.LoadInt64(&itemsProcessed)) / elapsedSeconds

	fmt.Printf("SinglePublisherRingBuffer Throughput Published: %.2f items/second\n", throughputPublished)
	fmt.Printf("SinglePublisherRingBuffer Throughput Processed: %.2f items/second\n", throughputProcessed)
	fmt.Printf("Items Published: %d, Items Processed: %d\n", itemsPublished, itemsProcessed)
}

func TestBatchPublisherRingBufferThroughput(t *testing.T) {
	bufferSize := uint64(1024 * 1024)
	batchSize := uint64(1000)
	numWorkers := 4
	duration := 5 * time.Second

	var itemsProcessed int64

	handler := func(items []interface{}) {
		atomic.AddInt64(&itemsProcessed, int64(len(items)))
	}

	rb := NewBatchPublisherRingBuffer(bufferSize, reflect.TypeOf(TestItem{}), batchSize, numWorkers, handler)

	start := time.Now()
	end := start.Add(duration)

	var itemsPublished int64
	batch := make([]interface{}, batchSize)
	for time.Now().Before(end) {
		for i := range batch {
			batch[i] = &TestItem{Value: rand.Int()}
		}
		published := rb.PublishBatch(batch)
		atomic.AddInt64(&itemsPublished, int64(published))
	}

	rb.Shutdown()
	rb.Wait()

	elapsedSeconds := time.Since(start).Seconds()
	throughputPublished := float64(itemsPublished) / elapsedSeconds
	throughputProcessed := float64(atomic.LoadInt64(&itemsProcessed)) / elapsedSeconds

	fmt.Printf("BatchPublisherRingBuffer Throughput Published: %.2f items/second\n", throughputPublished)
	fmt.Printf("BatchPublisherRingBuffer Throughput Processed: %.2f items/second\n", throughputProcessed)
	fmt.Printf("Items Published: %d, Items Processed: %d\n", itemsPublished, itemsProcessed)
}
