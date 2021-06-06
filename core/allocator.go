package core

import "sync"

type IngestBufferAllocatorIFace interface {
	Allocate(int64) *IngestBuffer
	Deallocate()
	SetMaxBuffers(int64)
}

type IngestBufferAllocator struct {
	cv             *sync.Cond
	currentBuffers int64
	maxBuffers     int64
}

func NewIngestBufferAllocator() *IngestBufferAllocator {
	mutex := sync.Mutex{}
	return &IngestBufferAllocator{
		cv:             sync.NewCond(&mutex),
		currentBuffers: 0,
		maxBuffers:     1,
	}
}

func (iba *IngestBufferAllocator) Allocate(capacity int64) *IngestBuffer {
	iba.cv.L.Lock()
	for iba.currentBuffers >= iba.maxBuffers {
		iba.cv.Wait()
	}
	iba.currentBuffers += 1
	iba.cv.L.Unlock()
	return NewIngestBuffer(capacity, iba)
}

func (iba *IngestBufferAllocator) Deallocate() {
	iba.cv.L.Lock()
	iba.currentBuffers -= 1
	iba.cv.Broadcast()
	iba.cv.L.Unlock()
}

func (iba *IngestBufferAllocator) SetMaxBuffers(maxBuffers int64) {
	iba.cv.L.Lock()
	iba.maxBuffers = maxBuffers
	iba.cv.Broadcast()
	iba.cv.L.Unlock()
}

// --- FOR TESTING ---

type TestAllocator struct {
	alloc         *IngestBufferAllocator
	totalAllocs   int
	totalDeallocs int
}

func NewTestAllocator() *TestAllocator {
	return &TestAllocator{
		alloc:         NewIngestBufferAllocator(),
		totalAllocs:   0,
		totalDeallocs: 0,
	}
}

func (ta *TestAllocator) Allocate(cap int64) *IngestBuffer {
	ta.totalAllocs += 1
	ib := ta.alloc.Allocate(cap)
	ib.allocator = ta
	return ib
}

func (ta *TestAllocator) Deallocate() {
	ta.totalDeallocs += 1
	ta.alloc.Deallocate()
}

func (ta *TestAllocator) SetMaxBuffers(maxBuffers int64) {
	ta.alloc.SetMaxBuffers(maxBuffers)
}
