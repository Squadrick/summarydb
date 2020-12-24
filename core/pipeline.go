package core

import (
	"context"
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

	bufferSize  int64
	numElements int64

	emptyBuffers    chan *IngestBuffer
	partialBuffers  chan *IngestBuffer
	summarizerQueue chan *IngestBuffer
	writerQueue     chan *SummaryWindow
	mergerQueue     chan *MergeEvent
}

func NewPipeline(windowing window.Windowing) *Pipeline {
	emptyBuffers := make(chan *IngestBuffer, QueueSize)
	partialBuffers := make(chan *IngestBuffer, QueueSize)
	summarizerQueue := make(chan *IngestBuffer, QueueSize)
	writerQueue := make(chan *SummaryWindow, QueueSize)
	mergerQueue := make(chan *MergeEvent, QueueSize)
	barrier := NewBarrier()
	pipeline := &Pipeline{
		streamWindowManager: nil,
		ingester:            NewIngester(emptyBuffers, summarizerQueue),
		summarizer:          NewSummarizer(barrier),
		writer:              NewWriter(barrier),
		merger:              NewMerger(windowing, 1, barrier),
		barrier:             barrier,
		windowing:           windowing,
		bufferSize:          0,
		numElements:         0,
		emptyBuffers:        emptyBuffers,
		partialBuffers:      partialBuffers,
		summarizerQueue:     summarizerQueue,
		writerQueue:         writerQueue,
		mergerQueue:         mergerQueue,
	}
	return pipeline
}

func (p *Pipeline) Run(ctx context.Context) {
	go p.summarizer.Run(ctx, p.summarizerQueue, p.writerQueue, p.emptyBuffers, p.partialBuffers)
	go p.writer.Run(ctx, p.writerQueue, p.mergerQueue)
	go p.merger.Run(ctx, p.mergerQueue)
}

func (p *Pipeline) SetWindowManager(manager *StreamWindowManager) {
	p.streamWindowManager = manager
	if p.bufferSize > 0 {
		p.summarizer.SetWindowManager(manager)
	}
	p.writer.SetWindowManager(manager)
	p.merger.SetWindowManager(manager)
}

func (p *Pipeline) Append(timestamp int64, value float64) {
	if p.bufferSize > 0 {
		p.ingester.Append(timestamp, value)
	} else {
		p.appendUnbuffered(timestamp, value)
	}
	p.numElements += 1
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

func (p *Pipeline) destroyEmptyBuffers() {
loop:
	for {
		select {
		case buffer := <-p.emptyBuffers:
			if buffer != nil {
				buffer.Clear()
			}
		default:
			break loop
		}
	}
}

func (p *Pipeline) Flush(shutdown bool, setUnbuffered bool) {
	p.ingester.Flush(shutdown)
	p.barrier.Wait(SUMMARIZER)

	if p.bufferSize > 0 {
	loop:
		for {
			select {
			case partialBuffer := <-p.partialBuffers:
				if partialBuffer != nil {
					p.numElements -= partialBuffer.Size
					for i := int64(0); i < partialBuffer.Size; i++ {
						timestamp, _ := partialBuffer.GetTimestamp(i)
						value, _ := partialBuffer.GetValue(i)
						p.appendUnbuffered(timestamp, value)
						p.numElements += 1
					}
					partialBuffer.Clear()
					p.emptyBuffers <- partialBuffer
				}
			default:
				break loop
			}
		}
	}
	if setUnbuffered {
		p.bufferSize = 0
		p.destroyEmptyBuffers()
	}

	if shutdown {
		p.writerQueue <- ConstShutdownSummaryWindow()
		p.barrier.Wait(WRITER)
		p.mergerQueue <- ConstShutdownMergeEvent()
		p.barrier.Wait(MERGER)
	} else {
		p.writerQueue <- ConstFlushSummaryWindow()
		p.barrier.Wait(WRITER)
		p.mergerQueue <- ConstFlushMergeEvent()
		p.barrier.Wait(MERGER)
	}
}

func (p *Pipeline) SetBufferSize(totalBufferSize int64, numBuffer int64) {
	p.destroyEmptyBuffers()

	bufferWindowLengths := p.windowing.GetWindowsCoveringUpto(totalBufferSize / numBuffer)
	p.summarizer.SetWindowLengths(bufferWindowLengths)

	for _, length := range bufferWindowLengths {
		p.bufferSize += length
	}

	if p.bufferSize > 0 {
		for i := int64(0); i < numBuffer; i++ {
			p.emptyBuffers <- NewIngestBuffer(p.bufferSize)
		}
	}
}

func (p *Pipeline) SetWindowsPerBatch(windowsPerBatch int64) {
	p.merger.windowsPerBatch = windowsPerBatch
}
