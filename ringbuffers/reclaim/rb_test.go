package main

import (
	"fmt"
	"sync"
	"testing"
)

func BenchmarkLoad100k(b *testing.B) {
	for i := 0; i < b.N; i++ {
		rb := NewRingBuffer[int]()
		defer rb.Close()

		elements := 100_000
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			many := []int{}
			for i := 0; i < 100_000; i++ {
				many = append(many, i)
			}
			rb.EnqueueMany(many)
		}()

		total := 0

		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-rb.notifier:
					if !rb.Has() {
						return
					}
					msgs, ok := rb.DequeueMany(messageBatchSize)
					if !ok {
						fmt.Println("error dequeue")
						continue
					}
					total += len(msgs)
					if total == elements {
						return
					}
				}
			}
		}()

		wg.Wait()
	}
}

func BenchmarkLoad1M(b *testing.B) {
	for i := 0; i < b.N; i++ {
		rb := NewRingBuffer[int]()
		defer rb.Close()

		elements := 1_000_000

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			many := []int{}
			for i := 0; i < elements; i++ {
				many = append(many, i)
			}
			rb.EnqueueMany(many)
		}()

		total := 0

		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-rb.notifier:
					if !rb.Has() {
						return
					}
					msgs, ok := rb.DequeueMany(messageBatchSize)
					if !ok {
						fmt.Println("error dequeue")
						continue
					}
					total += len(msgs)
					if total == elements {
						return
					}
				}
			}
		}()

		wg.Wait()
	}
}
