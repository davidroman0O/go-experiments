package main

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

type RuntimeFunc func(ctx context.Context) error

type Metadata struct {
	Attempts int
}

type Message struct {
	Topic    string
	Payload  interface{}
	Metadata Metadata
}

type DeadLetter struct {
	Recipient string
	Reason    string
}

func NewDeadLetter(recipient, reason string) DeadLetter {
	return DeadLetter{
		Recipient: recipient,
		Reason:    reason,
	}
}

type runtimeContext struct {
	shutdown chan struct{}
	mailbox  chan Message
}

type Library struct {
	functions  sync.Map
	contexts   sync.Map
	dequeues   sync.Map
	queues     sync.Map
	systemChan chan Message
	buffer     chan Message
	wg         sync.WaitGroup
	isShutdown atomic.Bool
	done       chan struct{}
}

func NewLibrary() *Library {
	return &Library{
		systemChan: make(chan Message, 1024),
		buffer:     make(chan Message, 1024*8),
		done:       make(chan struct{}),
	}
}

func (l *Library) handleBuffer() {
	ticker := time.NewTicker(1 * time.Microsecond)
	defer ticker.Stop()

	for {
		select {
		case msg, ok := <-l.buffer:
			if !ok {
				return // Buffer channel closed, exit the goroutine
			}
			msg.Metadata.Attempts++
			if msg.Metadata.Attempts < 3 {
				if msg.Topic == "" {
					l.Post(msg)
				} else {
					l.Put(msg)
				}
			} else {
				l.PostSystem(Message{
					Topic: "system",
					Payload: NewDeadLetter(
						msg.Topic,
						fmt.Sprintf("Failed to deliver after %d attempts", msg.Metadata.Attempts),
					),
				})
			}
		case <-ticker.C:
			if l.isShutdown.Load() {
				return // Library is shutting down, exit the goroutine
			}
		}
	}
}

func (l *Library) handleSystem() {
	for {
		select {
		case msg, ok := <-l.systemChan:
			if !ok {
				return // System channel closed, exit the goroutine
			}
			switch payload := msg.Payload.(type) {
			case DeadLetter:
				fmt.Printf("Dead letter for %s: %s\n", payload.Recipient, payload.Reason)
				if ctx, ok := l.contexts.Load(payload.Recipient); ok {
					rctx := ctx.(*runtimeContext)
					select {
					case <-rctx.shutdown:
						// Already closed
					default:
						close(rctx.shutdown)
					}
				}
			default:
				fmt.Printf("Unknown system message type: %T\n", payload)
			}
		case <-l.done:
			return // Library is done, exit the goroutine
		}
	}
}

func (l *Library) Register(name string, fn RuntimeFunc) {
	l.functions.Store(name, fn)
	l.contexts.Store(name, &runtimeContext{
		shutdown: make(chan struct{}),
		mailbox:  make(chan Message, 1024),
	})
}

func (l *Library) Run() {
	l.functions.Range(func(key, value interface{}) bool {
		name := key.(string)
		fn := value.(RuntimeFunc)
		l.wg.Add(1)
		go func() {
			defer l.wg.Done()
			ctx := context.Background()
			ctx = context.WithValue(ctx, "library", l)
			ctx = context.WithValue(ctx, "funcName", name)
			_ = fn(ctx)
		}()
		return true
	})
	go l.handleBuffer()
	go l.handleSystem()
}

func (l *Library) Await() {
	l.wg.Wait()
}

func (l *Library) Shutdown() {
	if !l.isShutdown.CompareAndSwap(false, true) {
		return // Already shut down
	}
	l.contexts.Range(func(_, value interface{}) bool {
		ctx := value.(*runtimeContext)
		select {
		case <-ctx.shutdown:
			// Channel is already closed, do nothing
		default:
			close(ctx.shutdown)
		}
		return true
	})
	l.wg.Wait()
	close(l.done)
	close(l.systemChan)
	close(l.buffer)
	// time.Sleep(100 * time.Millisecond)
}

func (l *Library) Post(msg Message) {
	if l.isShutdown.Load() {
		return
	}
	if ctx, ok := l.contexts.Load(msg.Topic); ok {
		select {
		case ctx.(*runtimeContext).mailbox <- msg:
		default:
			l.buffer <- msg
		}
	} else {
		l.PostSystem(Message{
			Topic:   "system",
			Payload: NewDeadLetter(msg.Topic, "Recipient not found"),
		})
	}
}

func (l *Library) Put(msg Message) {
	if l.isShutdown.Load() {
		return
	}
	var value any
	var ok bool
	if _, ok = l.queues.Load(msg.Topic); !ok {
		queue := NewQueue()

		if _, ok = l.dequeues.Load(msg.Topic); !ok {
			l.dequeues.Store(msg.Topic, make(chan Message, 1024))
		}

		// Create a custom dequeue function
		customDequeue := func(item interface{}) {
			// fmt.Printf("Dequeued item: %v\n", item)
			dequeueValue, _ := l.dequeues.Load(msg.Topic)
			dequeue := dequeueValue.(chan Message)
			dequeue <- item.(Message)
		}

		queueTicker := NewQueueTicker(queue, customDequeue)
		clock := New(WithInterval(1 * time.Millisecond))
		clock.Add(queueTicker, NonBlocking)
		clock.Start()

		l.queues.Store(msg.Topic, queue)
		value = queue
	} else {
		value, _ = l.queues.Load(msg.Topic)
	}
	queue := value.(*Queue)
	queue.Enqueue(msg)
}

func (l *Library) PostSystem(msg Message) {
	if l.isShutdown.Load() {
		return
	}
	l.systemChan <- msg
}

type SafeLibrary struct {
	*Library
	mu sync.RWMutex
}

func (sl *SafeLibrary) Post(msg Message) {
	sl.mu.RLock()
	defer sl.mu.RUnlock()
	sl.Library.Post(msg)
}

func (sl *SafeLibrary) Put(msg Message) {
	sl.mu.RLock()
	defer sl.mu.RUnlock()
	sl.Library.Put(msg)
}

func (sl *SafeLibrary) PostSystem(msg Message) {
	sl.mu.RLock()
	defer sl.mu.RUnlock()
	sl.Library.PostSystem(msg)
}

func UseEnqueue(ctx context.Context) func(Message) {
	return ctx.Value("library").(*Library).Put
}

func UseCourier(ctx context.Context) func(Message) {
	return ctx.Value("library").(*Library).Post
}

func UseMailbox(ctx context.Context) <-chan Message {
	lib := ctx.Value("library").(*Library)
	name := ctx.Value("funcName").(string)
	value, ok := lib.contexts.Load(name)
	if !ok {
		panic("Runtime context not found")
	}
	return value.(*runtimeContext).mailbox
}

func UseQueue(ctx context.Context, topic string) <-chan Message {
	lib := ctx.Value("library").(*Library)
	value, _ := lib.dequeues.LoadOrStore(topic, make(chan Message, 1024))
	return value.(chan Message)
}

func UseShutdown(ctx context.Context) <-chan struct{} {
	lib := ctx.Value("library").(*Library)
	name := ctx.Value("funcName").(string)
	value, ok := lib.contexts.Load(name)
	if !ok {
		panic("Runtime context not found")
	}
	return value.(*runtimeContext).shutdown
}

func UseDeadletter(ctx context.Context) func(string, string) {
	return func(recipient, reason string) {
		ctx.Value("library").(*Library).PostSystem(Message{
			Topic:   "system",
			Payload: NewDeadLetter(recipient, reason),
		})
	}
}

func pingFunc(ctx context.Context) error {
	fmt.Println("Ping started")
	mailbox := UseMailbox(ctx)
	shutdown := UseShutdown(ctx)
	// lib := UseLibrary(ctx)
	dead := UseDeadletter(ctx)
	enqueue := UseEnqueue(ctx)

	ticker := time.NewTicker(1 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			enqueue(Message{Topic: "ping-pong-queue", Payload: "ping"})
		case msg := <-mailbox:
			if msg.Payload == "pong" {
				atomic.AddInt32(&counter, 1)
				// fmt.Println("Ping received pong")
			}
		case <-shutdown:
			fmt.Println("Ping shutting down")
			dead("pong", "Ping is shutting down")
			return nil
		}
	}
}

func pongFunc(ctx context.Context) error {
	fmt.Println("Pong started")
	shutdown := UseShutdown(ctx)
	queue := UseQueue(ctx, "ping-pong-queue")
	courier := UseCourier(ctx)

	for {
		select {
		case msg := <-queue:
			if msg.Payload == "ping" {
				atomic.AddInt32(&counter, 1)
				// fmt.Println("Pong received ping", msg.Payload)
				courier(Message{Topic: "ping", Payload: "pong"})
			}
		case <-shutdown:
			fmt.Println("Pong shutting down")
			return nil
		}
	}
}

func calculateNumWorkers() int {
	numCPU := runtime.NumCPU()
	numWorkers := numCPU / 4
	if numWorkers == 0 {
		numWorkers = 1
	}
	return numWorkers
}

var numWorkers = calculateNumWorkers() // Number of worker goroutines

var counter int32

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	lib := NewLibrary()

	lib.Register("ping", pingFunc)
	lib.Register("pong", pongFunc)

	lib.Run()

	// Goroutine to send a dead letter after 150 milliseconds
	go func() {
		time.Sleep(1 * time.Second)
		fmt.Println("Sending dead letter")
		lib.PostSystem(Message{
			Topic:   "system",
			Payload: NewDeadLetter("ping", "Test dead letter"),
		})
	}()

	lib.Await()
	lib.Shutdown()
	fmt.Println("Main function exiting", atomic.LoadInt32(&counter))
}
