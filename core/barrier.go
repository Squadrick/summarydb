package core

import (
	"sync"
	"sync/atomic"
)

const (
	SUMMARIZER = iota
	WRITER     = iota
	MERGER     = iota
)

type Barrier struct {
	flushCount int32
	counters   [3]int64
	cond       sync.Cond
	mutex      sync.Mutex
}

func NewBarrier() *Barrier {
	return &Barrier{
		flushCount: 0,
		counters:   [3]int64{0, 0, 0},
	}
}

func (b *Barrier) Notify(barrierType int) {
	b.mutex.Lock()
	b.counters[barrierType] += 1
	b.mutex.Unlock()
	b.cond.Broadcast()
}

func (b *Barrier) Wait(barrierType int, threshold int64) {
	for {
		b.mutex.Lock()
		currentVale := b.counters[barrierType]
		b.mutex.Unlock()
		if currentVale < threshold {
			b.cond.Wait()
		}
	}
}

func (b *Barrier) GetNextFlush() int32 {
	return atomic.AddInt32(&b.flushCount, 1)
}
