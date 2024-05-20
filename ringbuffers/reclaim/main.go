package main

import (
	"context"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

/// I can't do a Michael-Scott queue algorithm because i will have issues with the GC maintaining nested arrays

func main() {

	numCores := runtime.NumCPU()
	runtime.GOMAXPROCS(numCores)

	rb := NewRingBuffer[int]()
	defer rb.Close()
	var wg sync.WaitGroup

	clockTimeout := context.Background()
	ctx, cancel := context.WithTimeout(clockTimeout, 10*time.Second)

	producers := 4
	elements := 1_000_000
	var produced uint32 = 0
	var total uint32 = 0

	// produce
	for p := 0; p < producers; p++ {
		wg.Add(1)
		go func(idx int) {
			for {
				select {
				case <-ctx.Done():
					wg.Done()
					return
				default:
					many := []int{}
					for i := 0; i < elements; i++ {
						many = append(many, i)
					}
					rb.Enqueue(many)
					atomic.AddUint32(&produced, uint32(elements))
				}
			}
		}(p)
	}

	consumers := 4

	closed := false
	closer := func() {
		if closed {
			return
		}
		closed = true
		for i := 0; i < consumers; i++ {
			wg.Done()
		}
	}

	now := time.Now()
	// consume
	for i := 0; i < consumers; i++ {
		sub := make(chan []int)
		rb.Subscribe(sub)
		wg.Add(1)
		go func(s chan []int) {
			for {
				select {
				case <-ctx.Done():
					wg.Done()
					return
				case msgs := <-s:
					atomic.AddUint32(&total, uint32(len(msgs)))
				}
			}
		}(sub)
	}

	go func() {
		ticker := time.NewTicker(10 * time.Nanosecond)
		for {
			select {
			case <-ticker.C:
				rb.Tick()
			case <-ctx.Done():
				ticker.Stop()
				closer() // stop everything
				return
			}
		}
	}()

	// every 5 seconds downsize the buffer
	go func() {
		ticker := time.NewTicker(2 * time.Second)
		for {
			select {
			case <-ticker.C:
				fmt.Println("current size", rb.Capacity(), rb.Length())
				rb.Downsize()
				fmt.Println("new size", rb.Capacity(), rb.Length())
			case <-ctx.Done():
				ticker.Stop()
				return
			}
		}
	}()

	wg.Wait()
	defer cancel()
	fmt.Println(
		"elapsed",
		time.Since(now),
		"produced",
		formatNumber(atomic.LoadUint32(&produced)),
		"consumed",
		formatNumber(atomic.LoadUint32(&total)))
	rb.Downsize()
	fmt.Println("final buffer size", rb.Capacity(), rb.Length(), defaultRingBufferSize)
}

func formatNumber(num uint32) string {
	str := fmt.Sprintf("%d", num)
	var result []string
	length := len(str)
	for i := 0; i < length; i++ {
		if i > 0 && (length-i)%3 == 0 {
			result = append(result, " ")
		}
		result = append(result, string(str[i]))
	}
	return strings.Join(result, "")
}
