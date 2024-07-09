package ringbuffer

import (
	"reflect"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

type SinglePublisherRingBuffer struct {
	*RingBuffer
	batchSize     uint64
	flushInterval time.Duration
	handler       func([]interface{})
	shutdown      chan struct{}
	workerWg      sync.WaitGroup
}

func NewSinglePublisherRingBuffer(bufferSize uint64, typ reflect.Type, batchSize uint64, flushInterval time.Duration, numWorkers int, handler func([]interface{})) *SinglePublisherRingBuffer {
	rb := &SinglePublisherRingBuffer{
		RingBuffer:    NewRingBuffer(bufferSize, typ),
		batchSize:     batchSize,
		flushInterval: flushInterval,
		handler:       handler,
		shutdown:      make(chan struct{}),
	}

	rb.workerWg.Add(numWorkers)
	for i := 0; i < numWorkers; i++ {
		go rb.consumeLoop()
	}

	return rb
}
func (rb *SinglePublisherRingBuffer) GetObject() interface{} {
	return rb.pool.Get()
}

func (rb *SinglePublisherRingBuffer) TryPublish(obj interface{}) bool {
	sequence := rb.producerSequence.get()
	gatingSequence := rb.gatingSequence.get()

	if sequence-gatingSequence >= rb.bufferSize {
		rb.pool.Put(obj)
		return false
	}

	index := sequence & rb.mask
	atomic.StorePointer(&rb.buffer[index], unsafe.Pointer(reflect.ValueOf(obj).Pointer()))
	rb.producerSequence.set(sequence + 1)
	return true
}

func (rb *SinglePublisherRingBuffer) consumeLoop() {
	defer rb.workerWg.Done()
	batch := make([]interface{}, rb.batchSize)
	timer := time.NewTimer(rb.flushInterval)
	defer timer.Stop()

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

				// We don't need to return objects to the pool here,
				// as they're already managed by the buffer

				newSequence := consumerSeq + batchSize
				rb.consumerSequence.set(newSequence)
				rb.gatingSequence.set(newSequence)
			}

			if batchSize == 0 {
				select {
				case <-timer.C:
				case <-rb.shutdown:
					return
				default:
					time.Sleep(time.Millisecond)
				}
			} else {
				timer.Reset(rb.flushInterval)
			}
		}
	}
}

func (rb *SinglePublisherRingBuffer) Shutdown() {
	close(rb.shutdown)
}

func (rb *SinglePublisherRingBuffer) Wait() {
	rb.workerWg.Wait()
}
