package main

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

func main() {

	numCores := runtime.NumCPU()
	runtime.GOMAXPROCS(numCores)

	q := make(chan int, 1024*8)

	var wg sync.WaitGroup
	var counter int32 = 0
	elements := 50_000_000
	wg.Add(1)
	now := time.Now()
	go func() {
		defer wg.Done()
		for i := 0; i < elements; i++ {
			q <- i
		}
		close(q) // Close the channel after sending all values
	}()
	readers := 1
	for i := 1; i < readers+1; i++ {
		wg.Add(1)
		go func(reader int) {
			defer wg.Done()
			for value := range q {
				_ = value // You can process the value here if needed
				atomic.AddInt32(&counter, 1)
			}
		}(i)
	}
	wg.Wait()
	fmt.Println("time:", time.Since(now))
	fmt.Println("counter:", counter)
}
