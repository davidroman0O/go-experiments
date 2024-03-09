package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"reflect"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"unsafe"
)

const (
	bufferSize     = 1024
	maxBatchSize   = 64
	maxUnprocessed = 512
)

var (
	numWorkers = runtime.GOMAXPROCS(0)
	logger     = slog.Default()
)

// Message represents a message sent to an actor
type Message interface{}

// MessageEnvelope wraps a message with metadata
type MessageEnvelope struct {
	Dest      string
	Node      string
	MessageID uint64
	Type      reflect.Type
	Payload   Message
}

// Actor represents an actor that receives messages via a channel
type Actor struct {
	id           string
	behaviors    map[reflect.Type]func(inbox <-chan MessageEnvelope, outbox chan<- MessageEnvelope)
	inbox        chan MessageEnvelope
	outbox       chan MessageEnvelope
	wg           sync.WaitGroup
	cleanup      sync.Once
	messageIDGen uint64
}

// NewActor creates a new actor and starts its message loop
func NewActor(id string) *Actor {
	a := &Actor{
		id:        id,
		behaviors: make(map[reflect.Type]func(inbox <-chan MessageEnvelope, outbox chan<- MessageEnvelope)),
		inbox:     make(chan MessageEnvelope, bufferSize),
		outbox:    make(chan MessageEnvelope, bufferSize),
	}

	a.wg.Add(1)
	go a.loop()
	return a
}

// RegisterBehavior registers a behavior for a specific message type
func (a *Actor) RegisterBehavior(behavior interface{}) {
	typ := reflect.TypeOf(behavior)
	if typ.Kind() != reflect.Func {
		panic("behavior must be a function")
	}
	a.behaviors[typ] = behavior.(func(inbox <-chan MessageEnvelope, outbox chan<- MessageEnvelope))
}

// Tell sends a message to the actor's inbox
func (a *Actor) Tell(msg Message) {
	envelope := MessageEnvelope{
		Dest:      a.id,
		MessageID: atomic.AddUint64(&a.messageIDGen, 1),
		Type:      reflect.TypeOf(msg),
		Payload:   msg,
	}
	a.inbox <- envelope
}

// Receive waits for a message from the actor's outbox
func (a *Actor) Receive() MessageEnvelope {
	return <-a.outbox
}

// Stop signals the actor to stop its message loop
func (a *Actor) Stop() {
	a.cleanup.Do(func() {
		close(a.inbox)
	})
	a.wg.Wait()
	close(a.outbox)
}

// loop is the message loop for the actor
func (a *Actor) loop() {
	defer a.wg.Done()
	defer close(a.outbox)
	for envelope := range a.inbox {
		behavior, ok := a.behaviors[envelope.Type]
		if ok {
			behavior(batchToChannel([]MessageEnvelope{envelope}), a.outbox)
		}
	}
}

// ActorManager manages a group of actors and handles message dispatching
type ActorManager struct {
	actors     map[string]*Actor
	actorsMu   sync.RWMutex
	dispatched uint64
	totalMsgs  uint64
	inboxes    []chan MessageEnvelope
	arenaPool  *sync.Pool
	wg         sync.WaitGroup
	ctx        context.Context
	cancel     context.CancelFunc
}

// NewActorManager creates a new actor manager
func NewActorManager() *ActorManager {
	ctx, cancel := context.WithCancel(context.Background())
	m := &ActorManager{
		actors:    make(map[string]*Actor),
		inboxes:   make([]chan MessageEnvelope, numWorkers),
		arenaPool: &sync.Pool{New: func() interface{} { return &arenaAllocator{} }},
		ctx:       ctx,
		cancel:    cancel,
	}

	for i := 0; i < numWorkers; i++ {
		m.inboxes[i] = make(chan MessageEnvelope, bufferSize)
	}

	m.wg.Add(numWorkers)
	for i := 0; i < numWorkers; i++ {
		go m.dispatchLoop(i)
	}
	return m
}

var workerID uint64

// dispatchLoop is a worker goroutine that dispatches messages to actors
func (m *ActorManager) dispatchLoop(workerID int) {
	defer m.wg.Done()
	inbox := m.inboxes[workerID]
	buffer := make([]MessageEnvelope, 0, maxBatchSize)
	unprocessed := 0

	for {
		select {
		case <-m.ctx.Done():
			return
		case msg, ok := <-inbox:
			if !ok {
				return
			}
			buffer = append(buffer, msg)
			if len(buffer) == maxBatchSize || unprocessed >= maxUnprocessed {
				m.dispatchBatch(buffer)
				buffer = buffer[:0]
				unprocessed = 0
			} else {
				unprocessed++
			}
		default:
			if len(buffer) > 0 {
				m.dispatchBatch(buffer)
				buffer = buffer[:0]
				unprocessed = 0
			}
			runtime.Gosched()
		}
	}
}

// dispatchBatch dispatches a batch of messages to actors
func (m *ActorManager) dispatchBatch(batch []MessageEnvelope) {
	m.actorsMu.RLock()
	defer m.actorsMu.RUnlock()
	for _, env := range batch {
		actor, ok := m.actors[env.Dest]
		if ok {
			actor.Tell(env.Payload)
			atomic.AddUint64(&m.dispatched, 1)
		}
	}
	atomic.AddUint64(&m.totalMsgs, uint64(len(batch)))
}

// RegisterActor registers an actor with the manager
func (m *ActorManager) RegisterActor(actor *Actor) {
	m.actorsMu.Lock()
	defer m.actorsMu.Unlock()
	m.actors[actor.id] = actor
}

// UnregisterActor unregisters an actor from the manager
func (m *ActorManager) UnregisterActor(id string) {
	m.actorsMu.Lock()
	defer m.actorsMu.Unlock()
	if actor, ok := m.actors[id]; ok {
		actor.Stop()
		delete(m.actors, id)
	}
}

// Tell sends a message to an actor managed by the manager
func (m *ActorManager) Tell(dest, node string, msg Message) {
	inbox := m.inboxes[int(atomic.AddUint64(&workerID, 1)%uint64(numWorkers))]
	envelope := MessageEnvelope{
		Dest:    dest,
		Node:    node,
		Type:    reflect.TypeOf(msg),
		Payload: m.arenaPool.Get().(*arenaAllocator).copyValue(msg),
	}
	select {
	case inbox <- envelope:
	default:
		// Discard message if inbox is full
	}
}

// Stop gracefully stops the actor manager and all its actors
func (m *ActorManager) Stop() {
	m.cancel()
	m.wg.Wait()
	m.actorsMu.Lock()
	defer m.actorsMu.Unlock()
	for _, actor := range m.actors {
		actor.Stop()
	}
}

// HTTPHandler is an HTTP handler for actor communication between nodes
type HTTPHandler struct {
	manager *ActorManager
}

// NewHTTPHandler creates a new HTTP handler
func NewHTTPHandler(manager *ActorManager) *HTTPHandler {
	return &HTTPHandler{manager: manager}
}

// ServeHTTP handles incoming HTTP requests
func (h *HTTPHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Handle incoming messages and forward them to the appropriate actor
	actorID := r.URL.Path[1:] // Extract actor ID from URL path
	msg := Message(r.URL.Query().Get("msg"))
	h.manager.Tell(actorID, "", msg)
	fmt.Fprintf(w, "Message sent to actor %s", actorID)
}

// arenaAllocator is an arena allocator for efficient memory allocation
type arenaAllocator struct {
	arena []byte
	off   int
}

// allocate allocates a block of memory from the arena
func (a *arenaAllocator) allocate(size int) []byte {
	if len(a.arena)-a.off < size {
		a.arena = make([]byte, size*2)
		a.off = 0
	}
	start := a.off
	a.off += size
	return a.arena[start:a.off]
}

// copyValue copies a value to the arena and returns a new copy
func (a *arenaAllocator) copyValue(val interface{}) interface{} {
	size := int(unsafe.Sizeof(val))
	ptr := unsafe.Pointer(&val)
	dst := a.allocate(size)
	src := (*[1000000]byte)(ptr)[:size:size]
	copy(dst, src[:])
	return *(*interface{})(unsafe.Pointer(&dst))
}

func batchToChannel(batch []MessageEnvelope) <-chan MessageEnvelope {
	ch := make(chan MessageEnvelope, len(batch))
	for _, msg := range batch {
		ch <- msg
	}
	close(ch)
	return ch
}

func main() {
	// Create an actor manager
	manager := NewActorManager()

	// Create some actors with different behaviors
	greeter := NewActor("greeter")
	greeter.RegisterBehavior(func(inbox <-chan MessageEnvelope, outbox chan<- MessageEnvelope) {
		for env := range inbox {
			name, ok := env.Payload.(string)
			if !ok {
				continue
			}
			outbox <- MessageEnvelope{
				Dest:    env.Dest,
				Type:    reflect.TypeOf(""),
				Payload: fmt.Sprintf("Hello, %s!", name),
			}
		}
	})
	manager.RegisterActor(greeter)

	printer := NewActor("printer")
	printer.RegisterBehavior(func(inbox <-chan MessageEnvelope, outbox chan<- MessageEnvelope) {
		for env := range inbox {
			logger.Info("Received message", "message", env.Payload)
		}
	})
	manager.RegisterActor(printer)

	// Send messages to actors
	manager.Tell("greeter", "", "Alice")
	greeterMsg := greeter.Receive()
	manager.Tell("printer", "", greeterMsg.Payload)

	// Gracefully shut down on signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
	logger.Info("Shutting down...")
	manager.Stop()
	logger.Info("Shutdown complete")
}
