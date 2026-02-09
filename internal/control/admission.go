package control

import (
	"math"
	"sync/atomic"
)

type Limit struct {
	size     int32
	capacity int32
	tokens   chan struct{}
}

func NewLimit(max int32) *Limit {
	if max <= 0 {
		max = math.MaxInt32
	}

	return &Limit{
		capacity: max,
		tokens:   make(chan struct{}, max),
	}
}

func (l *Limit) TryAcquire() bool {
	select {
	case l.tokens <- struct{}{}:
		atomic.AddInt32(&l.size, 1)
		return true

	default:
		return false
	}
}

func (l *Limit) Release(){
	select {
	case <-l.tokens:
		atomic.AddInt32(&l.size, -1)
	default:
		
	}
}

func (l *Limit) Size() int32 {
	return atomic.LoadInt32(&l.size)
}

func (l *Limit) Capacity() int32 {
	return l.capacity
}
