package core

import (
	"context"
	"summarydb/storage"
	"summarydb/window"
)

const QueueSize = 100

type Pipeline struct {
	streamWindowManager *StreamWindowManager
	ingester            *Ingester
	summarizer          *Summarizer
	writer              *Writer
	merger              *Merger
	barrier             *Barrier
	windowing           window.Windowing

	bufferSize    int64
	numElements   int64
	lastTimestamp int64

	partialBuffers  chan *IngestBuffer
	summarizerQueue chan *IngestBuffer
	writerQueue     chan *SummaryWindow
	mergerQueue     chan *MergeEvent
}

func NewPipeline(windowing window.Windowing) *Pipeline {
	partialBuffers := make(chan *IngestBuffer, QueueSize)
	summarizerQueue := make(chan *IngestBuffer, QueueSize)
	writerQueue := make(chan *SummaryWindow, QueueSize)
	mergerQueue := make(chan *MergeEvent, QueueSize)
	barrier := NewBarrier()
	pipeline := &Pipeline{
		streamWindowManager: nil,
		ingester:            NewIngester(summarizerQueue),
		summarizer:          NewSummarizer(barrier),
		writer:              NewWriter(barrier),
		merger:              NewMerger(windowing, 1, barrier),
		barrier:             barrier,
		windowing:           windowing,
		bufferSize:          0,
		numElements:         0,
		lastTimestamp:       0,
		partialBuffers:      partialBuffers,
		summarizerQueue:     summarizerQueue,
		writerQueue:         writerQueue,
		mergerQueue:         mergerQueue,
	}
	return pipeline
}

func (p *Pipeline) Run(ctx context.Context) {
	go p.summarizer.Run(ctx, p.summarizerQueue, p.writerQueue, p.partialBuffers)
	go p.writer.Run(ctx, p.writerQueue, p.mergerQueue)
	go p.merger.Run(ctx, p.mergerQueue)
}

func (p *Pipeline) Append(timestamp int64, value float64) {
	if timestamp < p.lastTimestamp {
		timestamp = p.lastTimestamp + 1
	}

	if p.bufferSize > 0 {
		p.ingester.Append(timestamp, value)
	} else {
		p.appendUnbuffered(timestamp, value)
	}
	p.numElements += 1
	p.lastTimestamp = timestamp
	p.streamWindowManager.PutCountAndTime(
		storage.Pipeline,
		p.numElements,
		timestamp)
}

func (p *Pipeline) appendUnbuffered(timestamp int64, value float64) {
	newWindow := NewSummaryWindow(timestamp, timestamp, p.numElements, p.numElements)
	p.streamWindowManager.InsertIntoSummaryWindow(newWindow, timestamp, value)
	p.streamWindowManager.PutSummaryWindow(newWindow)
	info := &MergeEvent{
		Id:   newWindow.TimeStart,
		Size: newWindow.CountEnd - newWindow.CountStart + 1,
	}
	p.merger.Process(info)
}

func (p *Pipeline) writeRemainingElementsInBuffer() {
	if p.bufferSize > 0 {
		for {
			select {
			case partialBuffer := <-p.partialBuffers:
				if partialBuffer != nil {
					p.numElements -= partialBuffer.Size
					for i := int64(0); i < partialBuffer.Size; i++ {
						timestamp, value, _ := partialBuffer.Get(i)
						p.appendUnbuffered(timestamp, value)
						p.numElements += 1
					}
					partialBuffer.Clear()
				}
			default:
				return
			}
		}
	}
}

func (p *Pipeline) Flush(shutdown bool) {
	var summaryWindowSentinel *SummaryWindow
	var mergeEventSentinel *MergeEvent
	if shutdown {
		summaryWindowSentinel = ConstShutdownSummaryWindow()
		mergeEventSentinel = ConstShutdownMergeEvent()
	} else {
		summaryWindowSentinel = ConstFlushSummaryWindow()
		mergeEventSentinel = ConstFlushMergeEvent()
	}

	p.ingester.Flush(shutdown)
	p.barrier.Wait(SUMMARIZER)
	p.writerQueue <- summaryWindowSentinel
	p.barrier.Wait(WRITER)
	p.mergerQueue <- mergeEventSentinel
	p.barrier.Wait(MERGER)

	// No batching while merging. Process the partial buffer elements
	// synchronously.
	windowsPerMerge := p.merger.windowsPerBatch
	p.SetWindowsPerMerge(1)
	p.writeRemainingElementsInBuffer()
	p.SetWindowsPerMerge(windowsPerMerge)
}

func (p *Pipeline) SetWindowManager(manager *StreamWindowManager) *Pipeline {
	p.streamWindowManager = manager
	p.summarizer.SetWindowManager(manager)
	p.writer.SetWindowManager(manager)
	p.merger.SetWindowManager(manager)
	return p
}

func (p *Pipeline) SetBufferSize(maxPerBufferSize int64) *Pipeline {
	// Ensure that each ingest buffer can be summarized into an integral
	// number of windows, without leaving anything behind, i.e., in normal
	// (non-flush) operation, there are no partial buffers.
	bufferWindowLengths := p.windowing.GetWindowsCoveringUpto(maxPerBufferSize)
	p.summarizer.SetWindowLengths(bufferWindowLengths)
	for _, length := range bufferWindowLengths {
		p.bufferSize += length
	}
	p.ingester.setBufferCapacity(p.bufferSize)
	return p
}

func (p *Pipeline) SetWindowsPerMerge(windowsPerMerge int64) *Pipeline {
	p.merger.windowsPerBatch = windowsPerMerge
	return p
}

func (p *Pipeline) SetUnbuffered() *Pipeline {
	p.bufferSize = 0
	return p
}

func (p *Pipeline) SetNumBuffers(numBuffers int64) *Pipeline {
	p.ingester.allocator.SetMaxBuffers(numBuffers)
	return p
}

func (p *Pipeline) PrimeUp() {
	if p.streamWindowManager == nil {
		panic("cannot prime without window manager")
	}
	p.numElements, p.lastTimestamp =
		p.streamWindowManager.GetCountAndTime(storage.Pipeline)
	// TODO: PrimeUp can fail for writer in unbuffered mode, since no count/time
	// is written.
	//p.writer.PrimeUp()
	p.merger.PrimeUp()

	// Restore here.
}
