package core

import "sync/atomic"

var (
	IngesterIdCounter int32 = 0
)

type IngestBuffer struct {
	Capacity   int64
	Size       int64
	id         int32
	timestamps []int64
	values     []float64
}

var shutdownIngestBuffer *IngestBuffer = nil
var flushIngestBuffer *IngestBuffer = nil

func ConstShutdownIngestBuffer() *IngestBuffer {
	if shutdownIngestBuffer == nil {
		shutdownIngestBuffer = NewIngestBuffer(0)
	}
	return shutdownIngestBuffer
}

func ConstFlushIngestBuffer() *IngestBuffer {
	if flushIngestBuffer == nil {
		flushIngestBuffer = NewIngestBuffer(0)
	}
	return flushIngestBuffer
}

func NewIngestBuffer(capacity int64) *IngestBuffer {
	return &IngestBuffer{
		Capacity:   capacity,
		Size:       0,
		id:         atomic.AddInt32(&IngesterIdCounter, 1),
		timestamps: make([]int64, capacity, capacity),
		values:     make([]float64, capacity, capacity),
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
	for i := int64(0); i < ib.Size-s; i += 1 {
		ib.timestamps[i] = ib.timestamps[i+s]
		ib.values[i] = ib.values[i+s]
	}
	ib.Size -= s
}

func (ib *IngestBuffer) Clear() {
	ib.Size = 0
}

func (ib *IngestBuffer) GetTimestamp(pos int64) (int64, bool) {
	if pos < 0 || pos >= ib.Size {
		return 0, false
	}
	return ib.timestamps[pos], true
}

func (ib *IngestBuffer) GetValue(pos int64) (float64, bool) {
	if pos < 0 || pos >= ib.Size {
		return 0, false
	}
	return ib.values[pos], true
}

type Ingester struct {
	activeBuffer    *IngestBuffer
	emptyBuffers    <-chan *IngestBuffer
	summarizerQueue chan<- *IngestBuffer
}

func NewIngester(inputCh <-chan *IngestBuffer, outputCh chan<- *IngestBuffer) *Ingester {
	return &Ingester{
		activeBuffer:    nil,
		emptyBuffers:    inputCh,
		summarizerQueue: outputCh,
	}
}

func (i *Ingester) PushActiveToQueue() {
	if i.activeBuffer != nil && i.activeBuffer.Size > 0 {
		i.summarizerQueue <- i.activeBuffer
		i.activeBuffer = nil
	}
}

func (i *Ingester) Append(timestamp int64, value float64) {
	for i.activeBuffer == nil {
		select {
		case activeBuffer := <-i.emptyBuffers:
			i.activeBuffer = activeBuffer
			break
		}
	}
	i.activeBuffer.Append(timestamp, value)
	if i.activeBuffer.IsFull() {
		i.PushActiveToQueue()
	}
}

func (i *Ingester) Flush(shutdown bool) {
	i.PushActiveToQueue()
	if shutdown {
		i.summarizerQueue <- ConstShutdownIngestBuffer()
	} else {
		i.summarizerQueue <- ConstFlushIngestBuffer()
	}
}
