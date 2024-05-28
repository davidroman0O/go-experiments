package main

// MultiRingBuffer manages multiple RingBuffers
type MultiRingBuffer struct {
	buffers []*RingBuffer
	next    int // For simple round-robin scheduling
}

func newMultiRingBuffer(numBuffers int, size int, quit chan struct{}) *MultiRingBuffer {
	buffers := make([]*RingBuffer, numBuffers)
	for i := range buffers {
		buffers[i] = newRingBuffer(size, quit)
	}
	return &MultiRingBuffer{buffers: buffers}
}

// Enqueue attempts to enqueue a message to one of the RingBuffers, using a simple round-robin strategy
func (mrb *MultiRingBuffer) enqueue(msg Message) bool {
	for i := 0; i < len(mrb.buffers); i++ {
		if mrb.buffers[mrb.next].enqueue(msg) {
			mrb.next = (mrb.next + 1) % len(mrb.buffers)
			return true
		}
		mrb.next = (mrb.next + 1) % len(mrb.buffers)
	}
	// All buffers are full
	return false
}

// EnqueueMany attempts to enqueue a batch of messages, distributing them across the buffers
func (mrb *MultiRingBuffer) enqueueMany(msgs []Message) bool {
	for _, msg := range msgs {
		if !mrb.enqueue(msg) {
			return false
		}
	}
	return true
}
