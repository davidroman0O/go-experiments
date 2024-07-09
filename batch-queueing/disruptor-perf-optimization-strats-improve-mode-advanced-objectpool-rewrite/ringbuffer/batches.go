package ringbuffer

import (
	"reflect"
	"runtime"
	"sync"
	"sync/atomic"
	"unsafe"
)

type BatchPublisherRingBuffer struct {
	*RingBuffer
	consumerSequences []*sequence
	batchSize         uint64
	numWorkers        int
	handler           func([]interface{})
	shutdown          chan struct{}
	workerWg          sync.WaitGroup
}

func NewBatchPublisherRingBuffer(bufferSize uint64, typ reflect.Type, batchSize uint64, numWorkers int, handler func([]interface{})) *BatchPublisherRingBuffer {
	rb := &BatchPublisherRingBuffer{
		RingBuffer:        NewRingBuffer(bufferSize, typ),
		consumerSequences: make([]*sequence, numWorkers),
		batchSize:         batchSize,
		numWorkers:        numWorkers,
		handler:           handler,
		shutdown:          make(chan struct{}),
	}

	for i := 0; i < numWorkers; i++ {
		rb.consumerSequences[i] = newSequence(0)
		rb.workerWg.Add(1)
		go rb.consumeLoop(i)
	}

	return rb
}

func (rb *BatchPublisherRingBuffer) PublishBatch(items []interface{}) (published int) {
	available := rb.bufferSize - (rb.producerSequence.get() - rb.gatingSequence.get())
	toPublish := min(uint64(len(items)), available)

	for i := uint64(0); i < toPublish; i++ {
		sequence := rb.producerSequence.get() + i
		index := sequence & rb.mask
		atomic.StorePointer(&rb.buffer[index], unsafe.Pointer(reflect.ValueOf(items[i]).Pointer()))
	}

	rb.producerSequence.set(rb.producerSequence.get() + toPublish)
	return int(toPublish)
}

func (rb *BatchPublisherRingBuffer) consumeLoop(workerID int) {
	defer rb.workerWg.Done()
	batch := make([]interface{}, rb.batchSize)

	for {
		select {
		case <-rb.shutdown:
			return
		default:
			producerSeq := rb.producerSequence.get()
			consumerSeq := rb.consumerSequence.get()
			available := producerSeq - consumerSeq
			batchSize := min(available, rb.batchSize)

			if batchSize > 0 {
				for i := uint64(0); i < batchSize; i++ {
					sequence := consumerSeq + i
					index := sequence & rb.mask
					batch[i] = reflect.NewAt(rb.typ, atomic.LoadPointer(&rb.buffer[index])).Interface()
				}

				rb.handler(batch[:batchSize])

				newSequence := consumerSeq + batchSize
				rb.consumerSequence.set(newSequence)
				rb.gatingSequence.set(newSequence)
			} else {
				runtime.Gosched()
			}
		}
	}
}

func (rb *BatchPublisherRingBuffer) updateGatingSequence() {
	min := rb.consumerSequences[0].get()
	for i := 1; i < rb.numWorkers; i++ {
		seq := rb.consumerSequences[i].get()
		if seq < min {
			min = seq
		}
	}
	rb.gatingSequence.set(min)
}

func (rb *BatchPublisherRingBuffer) Shutdown() {
	close(rb.shutdown)
}

func (rb *BatchPublisherRingBuffer) Wait() {
	rb.workerWg.Wait()
}

func min(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}
