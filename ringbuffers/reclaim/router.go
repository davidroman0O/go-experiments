package main

type MessageRouter[T any] struct {
	input         chan T
	consumers     []chan T
	consumerCount int
}

func NewMessageRouter[T any](consumerCount int) *MessageRouter[T] {
	input := make(chan T)
	consumers := make([]chan T, consumerCount)
	for i := range consumers {
		consumers[i] = make(chan T)
	}

	router := &MessageRouter[T]{
		input:         input,
		consumers:     consumers,
		consumerCount: consumerCount,
	}

	go router.run()

	return router
}

func (mr *MessageRouter[T]) Input() chan<- T {
	return mr.input
}

func (mr *MessageRouter[T]) Consumer(index int) <-chan T {
	return mr.consumers[index]
}

func (mr *MessageRouter[T]) run() {
	for msg := range mr.input {
		for i := 0; i < mr.consumerCount; i++ {
			mr.consumers[i] <- msg
		}
	}

	for i := 0; i < mr.consumerCount; i++ {
		close(mr.consumers[i])
	}
}
