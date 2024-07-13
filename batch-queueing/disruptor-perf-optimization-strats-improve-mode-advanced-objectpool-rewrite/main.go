package main

import (
	"sync"
)

type Systems struct {
	disruptors sync.Map
}

// - MPSC
// - RingBuffer based
// - Rolling RingBuffer based
// - Object pooling
// - single-publish single-laned
// - batch-publish single-laned
type Disruptor struct{}

func main() {

}
