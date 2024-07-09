package ringbuffer

import (
	"reflect"
	"sync"
	"sync/atomic"
	"unsafe"
)

type RingBuffer struct {
	buffer           []unsafe.Pointer
	mask             uint64
	producerSequence *sequence
	consumerSequence *sequence
	gatingSequence   *sequence
	bufferSize       uint64
	typ              reflect.Type
	pool             *sync.Pool
}

type sequence struct {
	value uint64
}

func newSequence(initial uint64) *sequence {
	return &sequence{value: initial}
}

func (s *sequence) get() uint64 {
	return atomic.LoadUint64(&s.value)
}

func (s *sequence) set(value uint64) {
	atomic.StoreUint64(&s.value, value)
}

func NewRingBuffer(bufferSize uint64, typ reflect.Type) *RingBuffer {
	size := nextPowerOfTwo(bufferSize)
	rb := &RingBuffer{
		buffer:           make([]unsafe.Pointer, size),
		mask:             size - 1,
		producerSequence: newSequence(0),
		consumerSequence: newSequence(0),
		gatingSequence:   newSequence(0),
		bufferSize:       size,
		typ:              typ,
		pool:             &sync.Pool{New: func() interface{} { return reflect.New(typ).Interface() }},
	}

	// Pre-allocate objects in the buffer using the pool
	for i := uint64(0); i < size; i++ {
		obj := rb.pool.Get()
		rb.buffer[i] = unsafe.Pointer(reflect.ValueOf(obj).Pointer())
	}

	return rb
}

func nextPowerOfTwo(v uint64) uint64 {
	v--
	v |= v >> 1
	v |= v >> 2
	v |= v >> 4
	v |= v >> 8
	v |= v >> 16
	v |= v >> 32
	v++
	return v
}
