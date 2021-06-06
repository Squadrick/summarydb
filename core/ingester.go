package core

import (
	"sync"
)

type IngestBuffer struct {
	Capacity   int64
	Size       int64
	timestamps []int64
	values     []float64
	allocator  IngestBufferAllocatorIFace
}

var shutdownIngestBuffer *IngestBuffer = nil
var shutdownIngestMutex sync.Mutex
var flushIngestBuffer *IngestBuffer = nil
var flushIngestMutex sync.Mutex

func ConstShutdownIngestBuffer() *IngestBuffer {
	shutdownIngestMutex.Lock()
	defer shutdownIngestMutex.Unlock()
	if shutdownIngestBuffer == nil {
		shutdownIngestBuffer = NewIngestBuffer(0, nil)
	}
	return shutdownIngestBuffer
}

func ConstFlushIngestBuffer() *IngestBuffer {
	flushIngestMutex.Lock()
	defer flushIngestMutex.Unlock()
	if flushIngestBuffer == nil {
		flushIngestBuffer = NewIngestBuffer(0, nil)
	}
	return flushIngestBuffer
}

func NewIngestBuffer(capacity int64, allocator IngestBufferAllocatorIFace) *IngestBuffer {
	return &IngestBuffer{
		Capacity:   capacity,
		Size:       0,
		timestamps: make([]int64, capacity, capacity),
		values:     make([]float64, capacity, capacity),
		allocator:  allocator,
	}
}

func (ib *IngestBuffer) Append(timestamp int64, value float64) bool {
	if ib.IsFull() {
		return false
	}

	ib.timestamps[ib.Size] = timestamp
	ib.values[ib.Size] = value
	ib.Size += 1
	return true
}

func (ib *IngestBuffer) IsFull() bool {
	return ib.Size == ib.Capacity
}

func (ib *IngestBuffer) TruncateHead(s int64) {
	if s == 0 {
		return
	}

	// Move elements from position s onwards to the start.
	copy(ib.timestamps, ib.timestamps[s:])
	copy(ib.values, ib.values[s:])
	ib.Size -= s
}

// Dealloc the underlying arrays.
func (ib *IngestBuffer) Clear() {
	ib.Size = 0
	ib.Capacity = 0
	ib.timestamps = nil
	ib.values = nil
	if ib.allocator != nil {
		ib.allocator.Deallocate()
		ib.allocator = nil
	}
}

func (ib *IngestBuffer) Get(pos int64) (int64, float64, bool) {
	if pos < 0 || pos >= ib.Size {
		return 0, 0, false
	}
	return ib.timestamps[pos], ib.values[pos], true
}

type Ingester struct {
	activeBuffer    *IngestBuffer
	allocator       *IngestBufferAllocator
	bufferCapacity  int64
	summarizerQueue chan<- *IngestBuffer
}

func NewIngester(outputCh chan<- *IngestBuffer) *Ingester {
	return &Ingester{
		activeBuffer:    nil,
		allocator:       NewIngestBufferAllocator(),
		bufferCapacity:  1, // use setBufferCapacity
		summarizerQueue: outputCh,
	}
}

func (i *Ingester) setBufferCapacity(cap int64) {
	i.bufferCapacity = cap
}

func (i *Ingester) pushActiveBufferToQueue() {
	if i.activeBuffer != nil && i.activeBuffer.Size > 0 {
		i.summarizerQueue <- i.activeBuffer
		i.activeBuffer = nil
	}
}

func (i *Ingester) Append(timestamp int64, value float64) {
	if i.activeBuffer == nil {
		i.activeBuffer = i.allocator.Allocate(i.bufferCapacity)
	}
	if i.activeBuffer.IsFull() {
		i.pushActiveBufferToQueue()
		i.activeBuffer = i.allocator.Allocate(i.bufferCapacity)
	}
	i.activeBuffer.Append(timestamp, value)
}

func (i *Ingester) Flush(shutdown bool) {
	i.pushActiveBufferToQueue()
	if shutdown {
		i.summarizerQueue <- ConstShutdownIngestBuffer()
	} else {
		i.summarizerQueue <- ConstFlushIngestBuffer()
	}
}
