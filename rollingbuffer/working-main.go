package main

// // LockFreeQueue is a simple, fast, and practical non-blocking queue.
// type LockFreeQueue[T any] struct {
// 	head unsafe.Pointer
// 	tail unsafe.Pointer
// }

// type node[T any] struct {
// 	value T
// 	next  unsafe.Pointer
// }

// // New creates a queue with dummy node.
// func New[T any]() *LockFreeQueue[T] {
// 	node := unsafe.Pointer(new(node[T]))
// 	return &LockFreeQueue[T]{
// 		head: node,
// 		tail: node,
// 	}
// }

// // Enqueue push back the given value v to queue.
// func (q *LockFreeQueue[T]) Enqueue(v T) {
// 	node := &node[T]{value: v}
// 	for {
// 		tail := load[T](&q.tail)
// 		next := load[T](&tail.next)
// 		if tail == load[T](&q.tail) {
// 			if next == nil {
// 				if cas(&tail.next, next, node) {
// 					cas(&q.tail, tail, node)
// 					return
// 				}
// 			} else {
// 				cas(&q.tail, tail, next)
// 			}
// 		}
// 	}
// }

// // Dequeue pop front a value from queue
// func (q *LockFreeQueue[T]) Dequeue() (v T, ok bool) {
// 	for {
// 		head := load[T](&q.head)
// 		tail := load[T](&q.tail)
// 		next := load[T](&head.next)
// 		if head == load[T](&q.head) {
// 			if head == tail {
// 				if next == nil {
// 					var zero T
// 					return zero, false
// 				}
// 				cas(&q.tail, tail, next)
// 			} else {
// 				v := next.value
// 				if cas(&q.head, head, next) {
// 					return v, true
// 				}
// 			}
// 		}
// 	}
// }

// func load[T any](p *unsafe.Pointer) *node[T] {
// 	return (*node[T])(atomic.LoadPointer(p))
// }

// func cas[T any](p *unsafe.Pointer, old, new *node[T]) bool {
// 	return atomic.CompareAndSwapPointer(p,
// 		unsafe.Pointer(old), unsafe.Pointer(new))
// }

// type RollingBuffer[T any] struct {
// 	queues         []*LockFreeQueue[T]
// 	lock           sync.Mutex
// 	producerWg     sync.WaitGroup
// 	consumerWg     sync.WaitGroup
// 	closeProducers int32
// 	closeConsumers int32
// 	currentQueue   int32
// }

// func NewRollingBuffer[T any]() *RollingBuffer[T] {
// 	return &RollingBuffer[T]{
// 		queues: []*LockFreeQueue[T]{
// 			New[T](),
// 		},
// 	}
// }

// func (rb *RollingBuffer[T]) NewProducerChannel() chan<- T {
// 	if atomic.LoadInt32(&rb.closeProducers) == 1 {
// 		return nil
// 	}

// 	ch := make(chan T, 1000) // Buffer size can be adjusted
// 	rb.producerWg.Add(1)

// 	go rb.producerWorker(ch)
// 	return ch
// }

// func (rb *RollingBuffer[T]) producerWorker(ch <-chan T) {
// 	defer rb.producerWg.Done()

// 	for value := range ch {
// 		if atomic.LoadInt32(&rb.closeProducers) == 1 {
// 			return
// 		}
// 		currentQueue := atomic.LoadInt32(&rb.currentQueue)
// 		rb.queues[currentQueue].Enqueue(value)
// 	}
// }

// func (rb *RollingBuffer[T]) NewConsumerChannel() <-chan T {
// 	if atomic.LoadInt32(&rb.closeConsumers) == 1 {
// 		return nil
// 	}

// 	ch := make(chan T, 1000) // Buffer size can be adjusted
// 	rb.consumerWg.Add(1)

// 	go rb.consumerWorker(ch)
// 	return ch
// }

// func (rb *RollingBuffer[T]) consumerWorker(ch chan<- T) {
// 	defer rb.consumerWg.Done()

// 	for {
// 		if atomic.LoadInt32(&rb.closeConsumers) == 1 {
// 			close(ch)
// 			return
// 		}

// 		valueDequeued := false
// 		rb.lock.Lock()
// 		for _, queue := range rb.queues {
// 			if value, ok := queue.Dequeue(); ok {
// 				ch <- value
// 				valueDequeued = true
// 				break
// 			}
// 		}
// 		rb.lock.Unlock()

// 		if !valueDequeued {
// 			time.Sleep(time.Millisecond) // Prevent tight loop
// 		}
// 	}
// }

// func (rb *RollingBuffer[T]) Close(waitForConsumers bool) {
// 	// Signal producers to stop
// 	atomic.StoreInt32(&rb.closeProducers, 1)

// 	fmt.Println("closing producers...")
// 	rb.producerWg.Wait()
// 	fmt.Println("closed producers")

// 	if waitForConsumers {
// 		rb.waitForQueuesEmpty()
// 	}

// 	// Signal consumers to stop
// 	atomic.StoreInt32(&rb.closeConsumers, 1)

// 	fmt.Println("closing consumers...")
// 	rb.consumerWg.Wait()
// 	fmt.Println("closed consumers")

// 	fmt.Println("RollingBuffer closed")
// }

// func (rb *RollingBuffer[T]) waitForQueuesEmpty() {
// 	rb.lock.Lock()
// 	defer rb.lock.Unlock()
// 	for {
// 		isEmpty := true
// 		for idx, queue := range rb.queues {
// 			head := load[int](&queue.head)
// 			tail := load[int](&queue.tail)
// 			if head.value != tail.value {
// 				isEmpty = false
// 				fmt.Println("queue not empty", idx, head.value, tail.value)
// 				break
// 			}
// 		}

// 		if isEmpty {
// 			break
// 		}

// 		fmt.Println("waiting for queues to be empty...")
// 		time.Sleep(10 * time.Millisecond) // Add a small delay
// 	}
// 	fmt.Println("queues are empty")
// }

// func main() {
// 	numCores := runtime.NumCPU()
// 	runtime.GOMAXPROCS(numCores)
// 	rollingBuffer := NewRollingBuffer[int]()
// 	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
// 	ctxEnd, cancelEnd := context.WithCancel(context.Background())
// 	defer cancel()
// 	defer cancelEnd()

// 	var counterConsumers int64
// 	var counterProducers int64
// 	start := time.Now()

// 	ticker := time.NewTicker(1 * time.Second)
// 	go func() {
// 		for {
// 			select {
// 			case <-ticker.C:
// 				fmt.Printf("Messages consumed: %d\n", atomic.LoadInt64(&counterConsumers))
// 				fmt.Printf("Messages produced: %d\n", atomic.LoadInt64(&counterProducers))
// 			case <-ctxEnd.Done():
// 				return
// 			}
// 		}
// 	}()

// 	// Create producers
// 	for i := 0; i < 200; i++ {
// 		producerChan := rollingBuffer.NewProducerChannel()
// 		go func(ch chan<- int) {
// 			for {
// 				select {
// 				case <-ctx.Done():
// 					close(ch)
// 					return
// 				default:
// 					ch <- 1
// 					atomic.AddInt64(&counterProducers, 1)
// 				}
// 			}
// 		}(producerChan)
// 	}

// 	// Create consumers
// 	for i := 0; i < 400; i++ {
// 		consumerChan := rollingBuffer.NewConsumerChannel()
// 		go func(ch <-chan int) {
// 			for {
// 				select {
// 				case <-ctx.Done():
// 					return
// 				case _, ok := <-ch:
// 					if !ok {
// 						return
// 					}
// 					atomic.AddInt64(&counterConsumers, 1)
// 				}
// 			}
// 		}(consumerChan)
// 	}

// 	<-ctx.Done()
// 	fmt.Println("closing")
// 	rollingBuffer.Close(true)

// 	cancelEnd()

// 	elapsed := time.Since(start).Seconds()
// 	fmt.Printf("Elapsed time: %.2f seconds\n", elapsed)
// 	fmt.Printf("Messages consumed per second: %.2f\n", float64(counterConsumers)/elapsed)
// 	fmt.Printf("Messages produced per second: %.2f\n", float64(counterProducers)/elapsed)
// }
