package core

import (
	"bytes"
	"context"
	"encoding/binary"
	"math/rand"
	"summarydb/storage"
	"summarydb/window"
	"sync/atomic"
	"time"
)

const QueueSize = 100

type Pipeline struct {
	streamWindowManager *StreamWindowManager
	wal                 *storage.Log

	ingester   *Ingester
	summarizer *Summarizer
	writer     *Writer
	merger     *Merger
	barrier    *Barrier
	windowing  window.Windowing

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
	//go p.flushWAL(ctx)
}

func (p *Pipeline) Append(timestamp int64, value float64) error {
	if timestamp < p.lastTimestamp {
		timestamp = p.lastTimestamp + 1
	}

	if p.bufferSize > 0 {
		p.ingester.Append(timestamp, value)
	} else {
		err := p.appendUnbuffered(timestamp, value)
		if err != nil {
			return err
		}
	}
	return p.appendWAL(timestamp, value)
}

func (p *Pipeline) appendWAL(timestamp int64, value float64) error {
	atomic.AddInt64(&p.numElements, 1)
	atomic.StoreInt64(&p.lastTimestamp, timestamp)
	if p.wal != nil {
		var buf bytes.Buffer
		_ = binary.Write(&buf, binary.LittleEndian, timestamp)
		_ = binary.Write(&buf, binary.LittleEndian, value)
		err := p.wal.Write(uint64(p.numElements), buf.Bytes())
		if err != nil {
			return err
		}
		return p.wal.Sync()
	}
	return nil
}

func (p *Pipeline) appendUnbuffered(timestamp int64, value float64) error {
	newWindow := NewSummaryWindow(timestamp, timestamp, p.numElements, p.numElements)
	p.streamWindowManager.InsertIntoSummaryWindow(newWindow, timestamp, value)
	err := p.streamWindowManager.PutSummaryWindow(newWindow)
	if err != nil {
		return err
	}
	info := &MergeEvent{
		Id:   newWindow.Id(),
		Size: newWindow.Size(),
	}
	return p.merger.Process(info)
}

func (p *Pipeline) writeRemainingElementsInBuffer() error {
	if p.bufferSize > 0 {
		for {
			select {
			case partialBuffer := <-p.partialBuffers:
				if partialBuffer != nil {
					p.numElements -= partialBuffer.Size
					for i := int64(0); i < partialBuffer.Size; i++ {
						timestamp, value, _ := partialBuffer.Get(i)
						err := p.appendUnbuffered(timestamp, value)
						if err != nil {
							return err
						}
						err = p.appendWAL(timestamp, value)
						if err != nil {
							return err
						}
					}
					partialBuffer.Clear()
				}
			default:
				return nil
			}
		}
	}
	return nil
}

func (p *Pipeline) flushWAL(ctx context.Context) {
	randomDelay := time.NewTicker(time.Duration(rand.Int31n(1000)+1) * time.Millisecond)
	for {
		select {
		case <-ctx.Done():
			return
		case <-randomDelay.C:
			_ = p.wal.Sync()
			randomDelay = time.NewTicker(time.Duration(rand.Int31n(10000)) * time.Millisecond)
		}
	}
}

func (p *Pipeline) Flush(shutdown bool) error {
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
	err := p.writeRemainingElementsInBuffer()
	if err != nil {
		return err
	}
	p.SetWindowsPerMerge(windowsPerMerge)
	if p.wal != nil {
		err = p.wal.Sync()
		if err != nil {
			return err
		}
	}
	return nil
}

func (p *Pipeline) SetWAL(wal *storage.Log) *Pipeline {
	p.wal = wal
	return p
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

func (p *Pipeline) ReadEntryFromWAL(idx uint64) (int64, float64, error) {
	var timestamp int64
	var value float64
	buf, err := p.wal.Read(idx)
	if err != nil {
		return 0, 0, err
	}
	bytesBuf := bytes.NewBuffer(buf)
	_ = binary.Read(bytesBuf, binary.LittleEndian, &timestamp)
	_ = binary.Read(bytesBuf, binary.LittleEndian, &value)
	return timestamp, value, nil
}

func (p *Pipeline) PrimeUp() error {
	if p.streamWindowManager == nil {
		panic("cannot prime without window manager")
	}
	{
		numElements, err := p.wal.LastIndex()
		if err != nil {
			return err
		}
		timestamp, _, err := p.ReadEntryFromWAL(numElements)
		p.numElements = int64(numElements)
		p.lastTimestamp = timestamp
		p.summarizer.numElements = p.numElements
	}

	{
		// TODO: Move this error ignore this writer.PrimeUp()
		_ = p.writer.PrimeUp() // PrimeUp can fail for writer in unbuffered mode,
		// since no count/time is written.
		err := p.merger.PrimeUp()
		if err != nil {
			return err
		}
	}
	return nil
}

func (p *Pipeline) Restore() error {
	if p.wal == nil {
		panic("cannot restore without wal")
	}
	mergerNum := p.merger.numElements
	writerNum := p.writer.numElements
	appendNum := p.numElements
	if writerNum == 0 {
		writerNum = appendNum
	}

	for n := mergerNum; n < writerNum; n++ {
		t, _, err := p.ReadEntryFromWAL(uint64(n))
		if err != nil {
			return err
		}
		summaryWindow, err := p.streamWindowManager.GetSummaryWindow(t)
		if err != nil {
			return err
		}
		mergerEvent := &MergeEvent{
			Id:   summaryWindow.Id(),
			Size: summaryWindow.Size(),
		}
		err = p.merger.Process(mergerEvent)
		if err != nil {
			return err
		}
	}

	for n := writerNum; n < appendNum; n++ {
		t, v, err := p.ReadEntryFromWAL(uint64(n))
		if err != nil {
			return err
		}
		err = p.appendUnbuffered(t, v)
		if err != nil {
			return err
		}
	}
	return nil
}
