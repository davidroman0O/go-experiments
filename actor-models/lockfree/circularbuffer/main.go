package main

import (
	"fmt"
	"sync/atomic"
	"time"
	"unsafe"
)

type lockFreeRingBuffer struct {
	buffer []unsafe.Pointer
	size   int
	head   atomic.Uint64
	tail   atomic.Uint64
}

func newLockFreeRingBuffer(size int) *lockFreeRingBuffer {
	r := &lockFreeRingBuffer{
		buffer: make([]unsafe.Pointer, size),
		size:   size,
	}
	return r
}

func (r *lockFreeRingBuffer) push(value interface{}) bool {
	valPtr := unsafe.Pointer(&value)
	for {
		tail := r.tail.Load()
		head := r.head.Load()
		nextTailIndex := (tail + 1) % uint64(r.size)

		if nextTailIndex == head%uint64(r.size) { // Buffer is full
			return false
		}

		if r.tail.CompareAndSwap(tail, nextTailIndex) {
			r.buffer[tail%uint64(r.size)] = valPtr
			return true
		}
	}
}

func (r *lockFreeRingBuffer) pop() (interface{}, bool) {
	for {
		head := r.head.Load()
		tail := r.tail.Load()
		if head%uint64(r.size) == tail%uint64(r.size) { // Buffer is empty
			return nil, false
		}

		valuePtr := r.buffer[head%uint64(r.size)]
		if r.head.CompareAndSwap(head, (head+1)%uint64(r.size)) {
			return *(*interface{})(valuePtr), true
		}
	}
}

func (r *lockFreeRingBuffer) popN(n int) ([]interface{}, bool) {
	items := make([]interface{}, 0, n)
	for i := 0; i < n; i++ {
		item, ok := r.pop()
		if !ok {
			break
		}
		items = append(items, item)
	}
	return items, len(items) > 0
}

func main() {
	r := newLockFreeRingBuffer(30000000)
	now := time.Now()
	for i := 0; i < 29999999; i++ {
		r.push("Hello")
		r.push("World")
		r.push("!")
	}

	items, ok := r.popN(50)
	if ok {
		fmt.Println(items)
		// for _, item := range items {
		// 	println("Popped", item.(string))
		// }
	}
	fmt.Println(time.Since(now))
}
