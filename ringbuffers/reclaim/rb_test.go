package main

const numMessages = 40000000 // Number of messages to process

// func BenchmarkLoad10DequeueMany0k(b *testing.B) {
// 	for i := 0; i < b.N; i++ {
// 		rb := NewRingBuffer[int]()
// 		defer rb.Close()

// 		elements := 100_000
// 		var wg sync.WaitGroup
// 		wg.Add(1)
// 		go func() {
// 			defer wg.Done()
// 			many := []int{}
// 			for i := 0; i < 100_000; i++ {
// 				many = append(many, i)
// 			}
// 			rb.Enqueue(many)
// 		}()

// 		total := 0

// 		wg.Add(1)
// 		go func() {
// 			defer wg.Done()
// 			for {
// 				select {
// 				case <-rb.notifier:
// 					if !rb.Has() {
// 						return
// 					}
// 					msgs, ok := rb.DequeueMany(defaultMessageBatchSize)
// 					if !ok {
// 						fmt.Println("error dequeue")
// 						continue
// 					}
// 					total += len(msgs)
// 					if total == elements {
// 						return
// 					}
// 				}
// 			}
// 		}()

// 		wg.Wait()
// 	}
// }

// func BenchmarkLoadDequeueMany1M(b *testing.B) {
// 	for i := 0; i < b.N; i++ {
// 		rb := NewRingBuffer[int]()
// 		defer rb.Close()

// 		elements := 1_000_000

// 		var wg sync.WaitGroup
// 		wg.Add(1)
// 		go func() {
// 			defer wg.Done()
// 			many := []int{}
// 			for i := 0; i < elements; i++ {
// 				many = append(many, i)
// 			}
// 			rb.EnqueueMany(many)
// 		}()

// 		total := 0

// 		wg.Add(1)
// 		go func() {
// 			defer wg.Done()
// 			for {
// 				select {
// 				case <-rb.notifier:
// 					if !rb.Has() {
// 						return
// 					}
// 					msgs, ok := rb.DequeueMany(defaultMessageBatchSize)
// 					if !ok {
// 						fmt.Println("error dequeue")
// 						continue
// 					}
// 					total += len(msgs)
// 					if total == elements {
// 						return
// 					}
// 				}
// 			}
// 		}()

// 		wg.Wait()
// 	}
// }

// func BenchmarkLoadSub4M(b *testing.B) {
// 	for i := 0; i < b.N; i++ {
// 		rb := NewRingBuffer[int]()
// 		defer rb.Close()
// 		var wg sync.WaitGroup

// 		producers := 4
// 		var elements uint32 = 4_000_000

// 		var total uint32 = 0

// 		// produce
// 		for p := 0; p < producers; p++ {
// 			wg.Add(1)
// 			go func(idx int) {
// 				defer wg.Done()
// 				many := []int{}
// 				var result float64 = float64(elements) / float64(producers)
// 				nums := int(math.Max(result, 1.0))

// 				for i := 0; i < nums; i++ {
// 					many = append(many, i)
// 					// rb.Enqueue(i)
// 				}

// 				rb.Enqueue(many)
// 				// fmt.Println("produced", idx, nums)
// 			}(p)
// 		}

// 		clockTimeout := context.Background()
// 		ctx, cancel := context.WithTimeout(clockTimeout, 10*time.Second)

// 		consumers := 4

// 		closed := false
// 		closer := func() {
// 			if closed {
// 				return
// 			}
// 			closed = true
// 			for i := 0; i < consumers; i++ {
// 				wg.Done()
// 			}
// 		}

// 		// consume
// 		for i := 0; i < consumers; i++ {
// 			sub := make(chan []int)
// 			rb.Subscribe(sub)
// 			wg.Add(1)
// 			go func(s chan []int) {
// 				for {
// 					select {
// 					case msgs := <-s:
// 						atomic.AddUint32(&total, uint32(len(msgs)))
// 						// fmt.Println("consumed", len(msgs), atomic.LoadUint32(&total))
// 						if atomic.LoadUint32(&total) == atomic.LoadUint32(&elements) {
// 							closer()
// 							return
// 						}
// 					}
// 				}
// 			}(sub)
// 		}

// 		go func() {
// 			ticker := time.NewTicker(10 * time.Nanosecond)
// 			for {
// 				select {
// 				case <-ticker.C:
// 					rb.Tick()
// 				case <-ctx.Done():
// 					ticker.Stop()
// 				}
// 			}
// 		}()

// 		wg.Wait()
// 		defer cancel()
// 	}
// }

// func BenchmarkLoadSub10M(b *testing.B) {
// 	for i := 0; i < b.N; i++ {
// 		rb := NewRingBuffer[int]()
// 		defer rb.Close()
// 		var wg sync.WaitGroup

// 		producers := 4
// 		var elements uint32 = 10_000_000

// 		var total uint32 = 0

// 		// produce
// 		for p := 0; p < producers; p++ {
// 			wg.Add(1)
// 			go func(idx int) {
// 				defer wg.Done()
// 				many := []int{}
// 				var result float64 = float64(elements) / float64(producers)
// 				nums := int(math.Max(result, 1.0))

// 				for i := 0; i < nums; i++ {
// 					many = append(many, i)
// 					// rb.Enqueue(i)
// 				}

// 				rb.Enqueue(many)
// 				// fmt.Println("produced", idx, nums)
// 			}(p)
// 		}

// 		clockTimeout := context.Background()
// 		ctx, cancel := context.WithTimeout(clockTimeout, 10*time.Second)

// 		consumers := 4

// 		closed := false
// 		closer := func() {
// 			if closed {
// 				return
// 			}
// 			closed = true
// 			for i := 0; i < consumers; i++ {
// 				wg.Done()
// 			}
// 		}

// 		// consume
// 		for i := 0; i < consumers; i++ {
// 			sub := make(chan []int)
// 			rb.Subscribe(sub)
// 			wg.Add(1)
// 			go func(s chan []int) {
// 				for {
// 					select {
// 					case msgs := <-s:
// 						atomic.AddUint32(&total, uint32(len(msgs)))
// 						// fmt.Println("consumed", len(msgs), atomic.LoadUint32(&total))
// 						if atomic.LoadUint32(&total) == atomic.LoadUint32(&elements) {
// 							closer()
// 							return
// 						}
// 					}
// 				}
// 			}(sub)
// 		}

// 		go func() {
// 			ticker := time.NewTicker(10 * time.Nanosecond)
// 			for {
// 				select {
// 				case <-ticker.C:
// 					rb.Tick()
// 				case <-ctx.Done():
// 					ticker.Stop()
// 				}
// 			}
// 		}()

// 		wg.Wait()
// 		defer cancel()
// 	}
// }
