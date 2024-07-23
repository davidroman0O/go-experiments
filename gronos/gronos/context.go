package gronos

import (
	"context"

	"github.com/davidroman0O/gronos/clock"
	"github.com/davidroman0O/gronos/ringbuffer"
)

type gronosKey string

// The Courier is a proxy to write on the router ringbuffer to be dispatched
type clockContext struct {
	clock *clock.Clock
}

var clockKey gronosKey = gronosKey("clock")

func withClock(parent context.Context, rb *clock.Clock) context.Context {

	ctx := context.WithValue(parent, clockKey, clockContext{
		clock: rb,
	})

	return ctx
}

func UseClock(ctx context.Context) (*clock.Clock, bool) {
	signalCtx, ok := ctx.Value(clockKey).(clockContext)
	if !ok {
		return nil, false
	}
	return signalCtx.clock, true
}

// The Courier is a proxy to write on the router ringbuffer to be dispatched
type courierContext struct {
	Push func(msg Message) error
}

var courierKey gronosKey = gronosKey("courier")

func withCourier(parent context.Context, rb *ringbuffer.RingBuffer[Message]) context.Context {

	anonFn := func() func(msg Message) error {
		return func(msg Message) error {
			return rb.Push(msg)
		}
	}

	ctx := context.WithValue(parent, courierKey, courierContext{
		Push: anonFn(),
	})

	return ctx
}

func UseCourier(ctx context.Context) (courier func(msg Message) error, ok bool) {
	signalCtx, ok := ctx.Value(courierKey).(courierContext)
	if !ok {
		return nil, false
	}
	return signalCtx.Push, true
}

type shutdownContext struct {
	shutdown *Signal
}

var shutdownKey gronosKey = gronosKey("shutdown")

func withShutdown(parent context.Context) context.Context {
	ctx := context.WithValue(parent, shutdownKey, shutdownContext{
		shutdown: newSignal(),
	})
	return ctx
}

func UseShutdown(ctx context.Context) (shutdown *Signal, ok bool) {
	signalCtx, ok := ctx.Value(shutdownKey).(shutdownContext)
	if !ok {
		return nil, false
	}
	return signalCtx.shutdown, true
}

type playPauseContext struct {
	context.Context
	done   chan struct{}
	pause  chan struct{}
	resume chan struct{}
}

type Pause <-chan struct{}
type Play <-chan struct{}

var getIDKey gronosKey = gronosKey("getID")
var pauseKey gronosKey = gronosKey("pause")

func withPlayPause(parent context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(parent)
	done := make(chan struct{})
	pause := make(chan struct{}, 1)
	resume := make(chan struct{}, 1)

	playPauseCtx := &playPauseContext{ctx, done, pause, resume}
	ctx = context.WithValue(ctx, pauseKey, playPauseCtx)

	return ctx, cancel
}

func UsePaused(ctx context.Context) (Pause, Play, bool) {
	playPauseCtx, ok := ctx.Value(pauseKey).(*playPauseContext)
	if !ok {
		return nil, nil, false
	}
	return Pause(playPauseCtx.pause), Play(playPauseCtx.resume), true
}

func UsePlayPause(ctx context.Context) (func(), func(), bool) {
	playPauseCtx, ok := ctx.Value(pauseKey).(*playPauseContext)
	if !ok {
		return nil, nil, false
	}

	pauseFunc := func() {
		select {
		case playPauseCtx.pause <- struct{}{}:
		default:
		}
	}

	resumeFunc := func() {
		select {
		case playPauseCtx.resume <- struct{}{}:
		default:
		}
	}

	return pauseFunc, resumeFunc, true
}

type mailboxContext struct {
	buffer *ringbuffer.RingBuffer[Message]
}

var mailboxKey gronosKey = gronosKey("mailbox")

func withMailbox(parent context.Context, box *ringbuffer.RingBuffer[Message]) context.Context {
	ctx := context.WithValue(parent, mailboxKey, mailboxContext{
		buffer: box,
	})
	return ctx
}

// Check messages when they are available
func UseMailbox(ctx context.Context) (<-chan []Message, bool) {
	mailboxCtx, ok := ctx.Value(mailboxKey).(mailboxContext)
	if !ok {
		return nil, false
	}
	return mailboxCtx.buffer.DataAvailable(), true
}

// Private mailbox with full control, which is not part of the developer experience and not part of the control flow.
func useMailbox(ctx context.Context) (*ringbuffer.RingBuffer[Message], bool) {
	mailboxCtx, ok := ctx.Value(mailboxKey).(mailboxContext)
	if !ok {
		return nil, false
	}
	return mailboxCtx.buffer, true
}
