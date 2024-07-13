package main

import (
	"reflect"
	"sync"
)

/// Even a messaging system need a messaging system... :D
/// The only required if to be have a full synchronization of all systems, it's fine to be slow as long everyone updates in the same time.
/// One might initialize resources when receiving an event.

// Message is the interface for all messages in the bus
type Message interface {
	Type() string
}

// MessageBus is a generic message bus
type MessageBus struct {
	subscribers sync.Map
	messagePool sync.Pool
}

// NewMessageBus creates a new message bus
func NewMessageBus() *MessageBus {
	return &MessageBus{
		subscribers: sync.Map{},
		messagePool: sync.Pool{
			New: func() interface{} {
				return new(Message)
			},
		},
	}
}

// Subscribe adds a subscriber for a specific message type
func (mb *MessageBus) Subscribe(messageType string, subscriber func(Message)) {
	var subscribers []func(Message)
	if s, ok := mb.subscribers.Load(messageType); ok {
		subscribers = s.([]func(Message))
	}
	subscribers = append(subscribers, subscriber)
	mb.subscribers.Store(messageType, subscribers)
}

// Unsubscribe removes a subscriber for a specific message type
func (mb *MessageBus) Unsubscribe(messageType string, subscriber func(Message)) {
	if s, ok := mb.subscribers.Load(messageType); ok {
		subscribers := s.([]func(Message))
		for i, sub := range subscribers {
			if reflect.ValueOf(sub).Pointer() == reflect.ValueOf(subscriber).Pointer() {
				// Remove the subscriber
				subscribers = append(subscribers[:i], subscribers[i+1:]...)
				break
			}
		}
		if len(subscribers) == 0 {
			mb.subscribers.Delete(messageType)
		} else {
			mb.subscribers.Store(messageType, subscribers)
		}
	}
}

// Publish sends a message to all subscribers of its type
func (mb *MessageBus) Publish(message Message) {
	if subscribers, ok := mb.subscribers.Load(message.Type()); ok {
		for _, subscriber := range subscribers.([]func(Message)) {
			go subscriber(message)
		}
	}
}
