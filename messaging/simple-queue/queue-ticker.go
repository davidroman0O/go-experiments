package main

import (
	"sync/atomic"
	"unsafe"
	// Import the package containing the Clock implementation
)

type Node struct {
	value interface{}
	next  unsafe.Pointer
}

type Queue struct {
	head   unsafe.Pointer
	tail   unsafe.Pointer
	length int64
}

func NewQueue() *Queue {
	node := unsafe.Pointer(&Node{})
	return &Queue{head: node, tail: node}
}

func (q *Queue) Enqueue(value interface{}) {
	new := &Node{value: value}
	for {
		tail := load(&q.tail)
		next := load(&tail.next)
		if tail == load(&q.tail) {
			if next == nil {
				if cas(&tail.next, next, new) {
					cas(&q.tail, tail, new)
					atomic.AddInt64(&q.length, 1)
					return
				}
			} else {
				cas(&q.tail, tail, next)
			}
		}
	}
}

func (q *Queue) Dequeue() interface{} {
	for {
		head := load(&q.head)
		tail := load(&q.tail)
		next := load(&head.next)
		if head == load(&q.head) {
			if head == tail {
				if next == nil {
					return nil
				}
				cas(&q.tail, tail, next)
			} else {
				value := next.value
				if cas(&q.head, head, next) {
					atomic.AddInt64(&q.length, -1)
					return value
				}
			}
		}
	}
}

func (q *Queue) Length() int64 {
	return atomic.LoadInt64(&q.length)
}

func load(p *unsafe.Pointer) (n *Node) {
	return (*Node)(atomic.LoadPointer(p))
}

func cas(p *unsafe.Pointer, old, new *Node) (ok bool) {
	return atomic.CompareAndSwapPointer(
		p, unsafe.Pointer(old), unsafe.Pointer(new))
}

type QueueTicker struct {
	queue          *Queue
	dequeueFunc    func(interface{})
	dequeueChannel chan interface{}
}

func NewQueueTicker(queue *Queue, dequeueFunc func(interface{})) *QueueTicker {
	qt := &QueueTicker{
		queue:          queue,
		dequeueFunc:    dequeueFunc,
		dequeueChannel: make(chan interface{}, 1024), // Buffered channel
	}
	go qt.processItems()
	return qt
}

func (qt *QueueTicker) Tick() {
	if item := qt.queue.Dequeue(); item != nil {
		select {
		case qt.dequeueChannel <- item:
			// Item sent to channel
		default:
			// Channel is full, re-enqueue the item
			qt.queue.Enqueue(item)
		}
	}
}

func (qt *QueueTicker) processItems() {
	for item := range qt.dequeueChannel {
		qt.dequeueFunc(item)
	}
}

// func main() {
//     queue := NewQueue()

//     // Create a custom dequeue function
//     customDequeue := func(item interface{}) {
//         fmt.Printf("Dequeued item: %v\n", item)
//         // Simulate some processing time
//         time.Sleep(10 * time.Millisecond)
//     }

//     queueTicker := NewQueueTicker(queue, customDequeue)

//     // Set up the clock
//     clock := clock.New(clock.WithInterval(100 * time.Millisecond))
//     clock.Add(queueTicker, clock.NonBlocking)
//     clock.Start()

//     // Simulate multiple goroutines enqueuing
//     for i := 0; i < 5; i++ {
//         go func(id int) {
//             for j := 0; j < 10; j++ {
//                 queue.Enqueue(fmt.Sprintf("Item %d-%d", id, j))
//                 time.Sleep(50 * time.Millisecond)
//             }
//         }(i)
//     }

//     // Let the program run for a while
//     time.Sleep(3 * time.Second)

//     // Stop the clock
//     clock.Stop()
// }
