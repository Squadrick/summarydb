package core

import "context"

// Summarizer takes a stream of IngestBuffer which contains an array of
// (timestamp, int64 value) pairs that are appended to the stream.
// It creates a SummaryWindow out of the raw pairs. To identify how
// each SummaryWindow ends to be bounded, i.e., the windowing info,
// it stores windowLengths which are the valid consecutive window sizes
// up to the max buffer size of IngestBuffer. This is calculated from
// the window.Windowing info provided by user. If the ingest buffer is not
// completely consumable (in-case of a flush), add the leftover buffer
// to the partialBuffers channel.
type Summarizer struct {
	streamWindowManager *StreamWindowManager
	windowLengths       []int64
	numElements         int64
	barrier             *Barrier
}

func NewSummarizer(barrier *Barrier) *Summarizer {
	return &Summarizer{
		streamWindowManager: nil,
		windowLengths:       nil,
		numElements:         0,
		barrier:             barrier,
	}
}

func (s *Summarizer) SetWindowManager(manager *StreamWindowManager) {
	s.streamWindowManager = manager
}

func (s *Summarizer) SetWindowLengths(windowLengths []int64) {
	s.windowLengths = windowLengths
}

func (s *Summarizer) getNumWindowsCovering(ib *IngestBuffer) int {
	if ib.IsFull() {
		// This branch is very likely, except when flushing.
		return len(s.windowLengths)
	}
	N := int64(0)
	for w := 0; w < len(s.windowLengths); w += 1 {
		N += s.windowLengths[w]
		if N > ib.Size {
			return w
		} else if N == ib.Size {
			return w + 1
		}
	}
	return 0 // unreachable code
}

func (s *Summarizer) flush() {
	if s.barrier != nil {
		s.barrier.Notify(SUMMARIZER)
	}
}

func (s *Summarizer) Run(
	ctx context.Context,
	summarizerQueue <-chan *IngestBuffer,
	writerQueue chan<- *SummaryWindow,
	partialBuffers chan<- *IngestBuffer) {

	for {
		select {
		case ingestBuffer := <-summarizerQueue:
			if ingestBuffer == ConstShutdownIngestBuffer() {
				s.flush()
				break
			} else if ingestBuffer == ConstFlushIngestBuffer() {
				s.flush()
				continue
			} else {
				numWindows := s.getNumWindowsCovering(ingestBuffer)
				iStart := int64(0) // denotes the buffer position till where elements are consumed
				iEnd := int64(0)

				for w := numWindows - 1; w >= 0; w -= 1 {
					iEnd = iStart + s.windowLengths[w] - 1
					ts, _, _ := ingestBuffer.Get(iStart)
					te, _, ok := ingestBuffer.Get(iEnd)

					if !ok {
						// *Only* consume full windows. Anything that's left
						// is considered a partial buffer. We only check for
						// iEnd, since there's no case where iEnd is within
						// buffer bounds but iStart is not.
						break
					}

					window := NewSummaryWindow(
						ts, te, s.numElements+iStart, s.numElements+iEnd)

					for i := iStart; i <= iEnd; i += 1 {
						// ingestBuffer[iStart:iEnd] are guaranteed to exist.
						timestamp, value, _ := ingestBuffer.Get(i)
						s.streamWindowManager.InsertIntoSummaryWindow(window, timestamp, value)
					}

					writerQueue <- window
					iStart = iEnd + 1
				}
				s.numElements += iStart

				if iStart != ingestBuffer.Size {
					ingestBuffer.TruncateHead(iStart)
					partialBuffers <- ingestBuffer
				} else {
					// buffer is empty.
					ingestBuffer.Clear()
				}
			}
		case <-ctx.Done():
			return
		}
	}
}
