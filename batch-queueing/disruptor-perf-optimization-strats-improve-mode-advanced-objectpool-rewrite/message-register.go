package main

import (
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
)

/// One of the core principle of that new design is to know in advance all the types we allow to be passed around.
/// We will instanciate RingBuffers for each of those types with their own workers.

func MessageType[T any]() func() (string, reflect.Type) {
	return func() (string, reflect.Type) {
		return reflect.TypeFor[T]().String(), reflect.TypeFor[T]()
	}
}

// RegisterMessage represents a registration event
type RegisterMessage struct {
	Name  string
	RType reflect.Type
}

func (RegisterMessage) Type() string { return "message.type.register" }

// UnregisterMessage represents an unregistration event
type UnregisterMessage struct {
	Name string
}

func (UnregisterMessage) Type() string { return "message.type.unregister" }

// MessageRegister now uses MessageBus
type MessageRegister struct {
	register sync.Map
	bus      *MessageBus
}

func NewMessageRegister() *MessageRegister {
	return &MessageRegister{
		register: sync.Map{},
		bus:      NewMessageBus(),
	}
}

type MessageSignature func() (string, reflect.Type)

type Registration MessageSignature
type Unregistration MessageSignature

func (m *MessageRegister) Register(registration Registration) error {
	name, rtype := registration()
	if _, ok := m.register.Load(name); ok {
		return fmt.Errorf("already registered")
	}
	m.register.Store(name, rtype)

	m.bus.Publish(RegisterMessage{Name: name, RType: rtype})
	return nil
}

func (m *MessageRegister) Unregister(unregistration Unregistration) {
	name, _ := unregistration()
	m.register.Delete(name)

	m.bus.Publish(UnregisterMessage{Name: name})
}

type MessageRegisterProxy struct {
	register *MessageRegister
	ready    atomic.Bool
}

func NewMessageRegisterProxy(register *MessageRegister) *MessageRegisterProxy {
	proxy := &MessageRegisterProxy{
		register: NewMessageRegister(),
		ready:    atomic.Bool{},
	}
	register.register.Range(func(key, value interface{}) bool {
		proxy.register.register.Store(key, value)
		return true
	})

	register.bus.Subscribe("message.type.register", proxy.OnRegister)
	register.bus.Subscribe("message.type.unregister", proxy.OnUnregister)

	return proxy
}

func (p *MessageRegisterProxy) OnRegister(message Message) {
	if rm, ok := message.(RegisterMessage); ok {
		p.register.register.Store(rm.Name, rm.RType)
	}
}

func (p *MessageRegisterProxy) OnUnregister(message Message) {
	if um, ok := message.(UnregisterMessage); ok {
		p.register.register.Delete(um.Name)
	}
}
