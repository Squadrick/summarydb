package core

import (
	"context"
	"github.com/stretchr/testify/assert"
	"summarydb/storage"
	"testing"
)

type SummarizerTest struct {
	summarizer         *Summarizer
	summaryQueue       chan *IngestBuffer
	writerQueue        chan *SummaryWindow
	partialBufferQueue chan *IngestBuffer
	ctx                context.Context
	cancelFunc         context.CancelFunc
	running            bool
	closed             bool
}

func NewSummarizerTest() *SummarizerTest {
	b := NewBarrier()
	summ := NewSummarizer(b)
	manager := NewStreamWindowManager(0, []string{"sum"})
	backend := storage.NewInMemoryBackend()
	backingStore := NewBackingStore(backend, false)
	manager.SetBackingStore(backingStore)
	summ.SetWindowManager(manager)
	summ.SetWindowLengths([]int64{2, 4, 6, 8})

	summQueue := make(chan *IngestBuffer, 100)
	writerQueue := make(chan *SummaryWindow, 100)
	partialBufferQueue := make(chan *IngestBuffer, 100)

	return &SummarizerTest{
		summarizer:         summ,
		summaryQueue:       summQueue,
		writerQueue:        writerQueue,
		partialBufferQueue: partialBufferQueue,
		ctx:                nil,
		cancelFunc:         nil,
		running:            false,
		closed:             false,
	}
}

func (st *SummarizerTest) Run() {
	ctx, cancelFunc := context.WithCancel(context.Background())
	st.ctx = ctx
	st.cancelFunc = cancelFunc
	st.running = true
	go st.summarizer.Run(ctx, st.summaryQueue, st.writerQueue, st.partialBufferQueue)
}

func (st *SummarizerTest) Close() {
	if !st.running {
		panic("not running")
	}
	st.summaryQueue <- ConstFlushIngestBuffer()
	st.summarizer.barrier.Wait(SUMMARIZER)
	st.cancelFunc()
	close(st.summaryQueue)
	close(st.writerQueue)
	close(st.partialBufferQueue)
	st.closed = true
}

func (st *SummarizerTest) Push(ib *IngestBuffer) {
	if !st.running {
		panic("not running")
	}
	st.summaryQueue <- ib
}

func (st *SummarizerTest) GetWriteWindows() []*SummaryWindow {
	if !st.closed {
		panic("not closed")
	}
	summaryWindows := make([]*SummaryWindow, 0)
	for sw := range st.writerQueue {
		summaryWindows = append(summaryWindows, sw)
	}
	return summaryWindows
}

func (st *SummarizerTest) GetPartialBuffers() []*IngestBuffer {
	if !st.closed {
		panic("not closed")
	}
	partialBuffers := make([]*IngestBuffer, 0)
	for ib := range st.partialBufferQueue {
		partialBuffers = append(partialBuffers, ib)
	}
	return partialBuffers
}

func assertSummaryWindow(t *testing.T,
	window *SummaryWindow,
	ts, te, cs, ce int) {
	assert.Equal(t, window.TimeStart, int64(ts))
	assert.Equal(t, window.TimeEnd, int64(te))
	assert.Equal(t, window.CountStart, int64(cs))
	assert.Equal(t, window.CountEnd, int64(ce))
}

func TestSummarizer_Simple(t *testing.T) {
	st := NewSummarizerTest()

	bufferAllocator := NewTestAllocator()
	bufferAllocator.SetMaxBuffers(1)
	buffer := bufferAllocator.Allocate(20)
	for i := 0; i < 20; i++ {
		buffer.Append(int64(i), float64(i))
	}

	st.Run()
	st.Push(buffer)
	st.Close()

	summaryWindows := st.GetWriteWindows()
	assert.Equal(t, len(summaryWindows), 4)
	assertSummaryWindow(t, summaryWindows[0], 0, 7, 0, 7)
	assertSummaryWindow(t, summaryWindows[1], 8, 13, 8, 13)
	assertSummaryWindow(t, summaryWindows[2], 14, 17, 14, 17)
	assertSummaryWindow(t, summaryWindows[3], 18, 19, 18, 19)

	partialBuffers := st.GetPartialBuffers()
	assert.Equal(t, len(partialBuffers), 0)
	assert.Equal(t, bufferAllocator.totalAllocs, 1)
	assert.Equal(t, bufferAllocator.totalDeallocs, 1)
}

func TestSummarizer_MultipleBuffers(t *testing.T) {
	st := NewSummarizerTest()

	st.Run()
	bufferAllocator := NewTestAllocator()
	bufferAllocator.SetMaxBuffers(10)

	for j := 0; j < 10; j++ {
		buffer := bufferAllocator.Allocate(20)
		for i := 0; i < 20; i++ {
			buffer.Append(int64(j*20+i), float64(j*20+i))
		}
		st.Push(buffer)
	}
	st.Close()

	summaryWindows := st.GetWriteWindows()
	assert.Equal(t, len(summaryWindows), 40)

	for j := 0; j < 10; j++ {
		assertSummaryWindow(t, summaryWindows[4*j+0], 20*j+0, 20*j+7, 20*j+0, 20*j+7)
		assertSummaryWindow(t, summaryWindows[4*j+1], 20*j+8, 20*j+13, 20*j+8, 20*j+13)
		assertSummaryWindow(t, summaryWindows[4*j+2], 20*j+14, 20*j+17, 20*j+14, 20*j+17)
		assertSummaryWindow(t, summaryWindows[4*j+3], 20*j+18, 20*j+19, 20*j+18, 20*j+19)
	}

	partialBuffers := st.GetPartialBuffers()
	assert.Equal(t, len(partialBuffers), 0)

	assert.Equal(t, bufferAllocator.totalAllocs, 10)
	assert.Equal(t, bufferAllocator.totalDeallocs, 10)
}

func TestSummarizer_Partial(t *testing.T) {
	st := NewSummarizerTest()

	bufferAllocator := NewTestAllocator()
	bufferAllocator.SetMaxBuffers(2)
	buffer := bufferAllocator.Allocate(20)
	for i := 0; i < 20; i++ {
		buffer.Append(int64(i), float64(i))
	}
	bufferPartial := bufferAllocator.Allocate(20)
	for i := 0; i < 10; i++ {
		bufferPartial.Append(int64(i+20), float64(i+20))
	}

	st.Run()
	st.Push(buffer)
	st.Push(bufferPartial)
	st.Close()

	summaryWindows := st.GetWriteWindows()
	assert.Equal(t, len(summaryWindows), 6)
	assertSummaryWindow(t, summaryWindows[0], 0, 7, 0, 7)
	assertSummaryWindow(t, summaryWindows[1], 8, 13, 8, 13)
	assertSummaryWindow(t, summaryWindows[2], 14, 17, 14, 17)
	assertSummaryWindow(t, summaryWindows[3], 18, 19, 18, 19)
	assertSummaryWindow(t, summaryWindows[4], 20, 23, 20, 23)
	assertSummaryWindow(t, summaryWindows[5], 24, 25, 24, 25)

	partialBuffers := st.GetPartialBuffers()
	partialBuffer := partialBuffers[0]
	assert.Equal(t, partialBuffer.Size, int64(4))
	assert.Equal(t, partialBuffer.timestamps[0], int64(26))
	assert.Equal(t, partialBuffer.timestamps[3], int64(29))

	assert.Equal(t, len(partialBuffers), 1)
	assert.Equal(t, bufferAllocator.totalAllocs, 2)
	assert.Equal(t, bufferAllocator.totalDeallocs, 1)
}
