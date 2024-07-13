package main

import (
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"
)

// Sample structs for testing
type SomeTypeStruct struct {
	Field1 int
	Field2 string
}

type AnotherTypeStruct struct {
	Field1 float64
	Field2 bool
}

func TestMessageRegister(t *testing.T) {
	fmt.Println("Starting TestMessageRegister")
	mr := NewMessageRegister()

	// Test Register
	fmt.Println("Registering SomeTypeStruct")
	err := mr.Register(MessageType[SomeTypeStruct]())
	if err != nil {
		fmt.Printf("Failed to register SomeTypeStruct: %v\n", err)
		t.Errorf("Failed to register: %v", err)
	} else {
		fmt.Println("Successfully registered SomeTypeStruct")
	}

	// Test duplicate registration
	fmt.Println("Attempting duplicate registration of SomeTypeStruct")
	err = mr.Register(MessageType[SomeTypeStruct]())
	if err == nil {
		fmt.Println("Error: Expected error on duplicate registration, got nil")
		t.Error("Expected error on duplicate registration, got nil")
	} else {
		fmt.Printf("Received expected error on duplicate registration: %v\n", err)
	}

	// Test Unregister
	fmt.Println("Unregistering SomeTypeStruct")
	mr.Unregister(MessageType[SomeTypeStruct]())

	// Verify unregistration
	_, ok := mr.register.Load("main.SomeTypeStruct")
	if ok {
		fmt.Println("Error: Expected 'main.SomeTypeStruct' to be unregistered, but it still exists")
		t.Error("Expected 'main.SomeTypeStruct' to be unregistered, but it still exists")
	} else {
		fmt.Println("Successfully unregistered SomeTypeStruct")
	}
	fmt.Println("TestMessageRegister completed")
}

func TestMessageRegisterProxy(t *testing.T) {
	fmt.Println("Starting TestMessageRegisterProxy")
	mr := NewMessageRegister()
	proxy := NewMessageRegisterProxy(mr)
	fmt.Println("Created MessageRegister and MessageRegisterProxy")

	// Test propagation of Register event
	fmt.Println("Registering SomeTypeStruct")
	err := mr.Register(MessageType[SomeTypeStruct]())
	if err != nil {
		fmt.Printf("Failed to register SomeTypeStruct: %v\n", err)
		t.Errorf("Failed to register: %v", err)
	} else {
		fmt.Println("Successfully registered SomeTypeStruct")
	}

	// Give some time for propagation
	fmt.Println("Waiting for propagation...")
	time.Sleep(10 * time.Millisecond)

	// Verify proxy received the registration
	value, ok := proxy.register.register.Load("main.SomeTypeStruct")
	if !ok {
		fmt.Println("Error: Proxy did not receive Register event")
		t.Error("Proxy did not receive Register event")
	} else {
		fmt.Printf("Proxy received registration for 'main.SomeTypeStruct': %v\n", value)
		if value != reflect.TypeOf(SomeTypeStruct{}) {
			fmt.Printf("Error: Proxy received incorrect type. Expected SomeTypeStruct, got %v\n", value)
			t.Errorf("Proxy received incorrect type for 'main.SomeTypeStruct'. Expected SomeTypeStruct, got %v", value)
		} else {
			fmt.Println("Proxy received correct type for SomeTypeStruct")
		}
	}

	// Test propagation of Unregister event
	fmt.Println("Unregistering SomeTypeStruct")
	mr.Unregister(MessageType[SomeTypeStruct]())

	// Give some time for propagation
	fmt.Println("Waiting for propagation...")
	time.Sleep(10 * time.Millisecond)

	// Verify proxy received the unregistration
	_, ok = proxy.register.register.Load("main.SomeTypeStruct")
	if ok {
		fmt.Println("Error: Proxy did not receive Unregister event")
		t.Error("Proxy did not receive Unregister event")
	} else {
		fmt.Println("Proxy successfully unregistered SomeTypeStruct")
	}
	fmt.Println("TestMessageRegisterProxy completed")
}

func TestMultipleRegistrationsWithoutProxy(t *testing.T) {
	fmt.Println("Starting TestMultipleRegistrationsWithoutProxy")
	mr := NewMessageRegister()

	types := []Registration{
		MessageType[SomeTypeStruct](),
		MessageType[AnotherTypeStruct](),
		MessageType[int](),
		MessageType[string](),
		MessageType[float64](),
	}

	fmt.Println("Registering multiple types")
	for _, register := range types {
		name, _ := register()
		fmt.Printf("Registering type: %s\n", name)
		err := mr.Register(register)
		if err != nil {
			fmt.Printf("Failed to register %s: %v\n", name, err)
			t.Errorf("Failed to register: %v", err)
		} else {
			fmt.Printf("Successfully registered %s\n", name)
		}
	}

	// Verify all types are registered
	fmt.Println("Verifying all types are registered")
	for _, register := range types {
		name, _ := register()
		_, ok := mr.register.Load(name)
		if !ok {
			fmt.Printf("Error: Type %s was not registered\n", name)
			t.Errorf("Type %s was not registered", name)
		} else {
			fmt.Printf("Verified: %s is registered\n", name)
		}
	}

	// Unregister one type
	fmt.Println("Unregistering SomeTypeStruct")
	mr.Unregister(MessageType[SomeTypeStruct]())

	// Verify it's unregistered
	_, ok := mr.register.Load("main.SomeTypeStruct")
	if ok {
		fmt.Println("Error: SomeTypeStruct should have been unregistered")
		t.Error("SomeTypeStruct should have been unregistered")
	} else {
		fmt.Println("Successfully unregistered SomeTypeStruct")
	}
	fmt.Println("TestMultipleRegistrationsWithoutProxy completed")
}

func TestConcurrentRegistrationsWithProxy(t *testing.T) {
	fmt.Println("Starting TestConcurrentRegistrationsWithProxy")
	mr := NewMessageRegister()
	proxy := NewMessageRegisterProxy(mr)
	fmt.Println("Created MessageRegister and MessageRegisterProxy")

	types := []Registration{
		MessageType[SomeTypeStruct](),
		MessageType[AnotherTypeStruct](),
		MessageType[int](),
		MessageType[string](),
		MessageType[float64](),
	}

	var wg sync.WaitGroup
	wg.Add(len(types))

	fmt.Println("Starting concurrent registrations")
	for _, register := range types {
		go func(r Registration) {
			defer wg.Done()
			name, _ := r()
			err := mr.Register(r)
			if err != nil {
				fmt.Printf("Failed to register %s: %v\n", name, err)
			} else {
				fmt.Printf("Successfully registered %s\n", name)
			}
		}(register)
	}

	wg.Wait()
	fmt.Println("Concurrent registrations completed")

	// Give some time for propagation
	fmt.Println("Waiting for propagation...")
	time.Sleep(100 * time.Millisecond)

	// Verify all types are registered in both mr and proxy
	fmt.Println("Verifying registrations in MessageRegister and Proxy")
	for _, register := range types {
		name, _ := register()
		_, okMR := mr.register.Load(name)
		_, okProxy := proxy.register.register.Load(name)
		if !okMR || !okProxy {
			fmt.Printf("Error: Type %s was not registered in both mr and proxy\n", name)
			t.Errorf("Type %s was not registered in both mr and proxy", name)
		} else {
			fmt.Printf("Verified: %s is registered in both MessageRegister and Proxy\n", name)
		}
	}

	// Concurrent unregistration of half the types
	fmt.Println("Starting concurrent unregistrations for half the types")
	wg.Add(len(types) / 2)
	for i := 0; i < len(types)/2; i++ {
		go func(r Registration) {
			defer wg.Done()
			name, _ := r()
			mr.Unregister(func() (string, reflect.Type) {
				return r()
			})
			fmt.Printf("Unregistered %s\n", name)
		}(types[i])
	}

	wg.Wait()
	fmt.Println("Concurrent unregistrations completed")

	// Give some time for propagation
	fmt.Println("Waiting for propagation...")
	time.Sleep(100 * time.Millisecond)

	// Verify unregistered types are removed and others remain
	fmt.Println("Verifying final state")
	for i, register := range types {
		name, _ := register()
		_, okMR := mr.register.Load(name)
		_, okProxy := proxy.register.register.Load(name)
		if i < len(types)/2 {
			if okMR || okProxy {
				fmt.Printf("Error: Type %s should have been unregistered\n", name)
				t.Errorf("Type %s should have been unregistered", name)
			} else {
				fmt.Printf("Verified: %s is unregistered\n", name)
			}
		} else {
			if !okMR || !okProxy {
				fmt.Printf("Error: Type %s should still be registered\n", name)
				t.Errorf("Type %s should still be registered", name)
			} else {
				fmt.Printf("Verified: %s is still registered\n", name)
			}
		}
	}
	fmt.Println("TestConcurrentRegistrationsWithProxy completed")
}
