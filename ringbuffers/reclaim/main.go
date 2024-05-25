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
/// Should have one producer of a lot of messages and many consumers

func main() {

	numCores := runtime.NumCPU()
	runtime.GOMAXPROCS(numCores)

	clockTimeout := context.Background()
	ctx, cancel := context.WithTimeout(clockTimeout, 10*time.Second)

	rb := NewRingBuffer[int](
		ctx,
		WithSize(defaultRingBufferSize*4),
	)
	defer rb.Close()
	var wg sync.WaitGroup

	producers := 1
	fmt.Println("producers", producers)
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

	consumers := numCores
	fmt.Println("consumers", consumers)

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

	var counter uint64

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
					atomic.AddUint64(&counter, 1)
					atomic.AddUint32(&total, uint32(len(msgs)))
				}
			}
		}(sub)
	}

	go func() {
		ticker := time.NewTicker(10 * time.Nanosecond)
		tickerNorm := time.NewTicker(1 * time.Second / 2)
		for {
			select {
			case <-ticker.C:
				rb.Tick()
			case <-tickerNorm.C:
				fmt.Println("normalize delta", rb.NormalizedDelta())
				fmt.Println("delta", rb.Delta())
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
				fmt.Println("BEFORE capacity:", rb.Capacity(), " - length", rb.Length())
				rb.Downsize()
				fmt.Println("AFTER capacity:", rb.Capacity(), " - length", rb.Length())
			case <-ctx.Done():
				ticker.Stop()
				return
			}
		}
	}()

	fmt.Println("waiting for producers and consumers to finish")
	wg.Wait()
	fmt.Println("closing")
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
	fmt.Println("counter", atomic.LoadUint64(&counter))
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
