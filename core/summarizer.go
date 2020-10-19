package core

import "context"

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

func (s *Summarizer) Run(
	ctx context.Context,
	summarizerQueue <-chan *IngestBuffer,
	writerQueue chan<- *SummaryWindow,
	emptyBuffers chan<- *IngestBuffer,
	partialBuffers chan<- *IngestBuffer) {

	for {
		select {
		case ingestBuffer := <-summarizerQueue:
			if ingestBuffer == ConstShutdownIngestBuffer() {
				if s.barrier != nil {
					s.barrier.Notify(SUMMARIZER)
				}
				break
			} else if ingestBuffer == ConstFlushIngestBuffer() {
				if s.barrier != nil {
					s.barrier.Notify(SUMMARIZER)
				}
				continue
			} else {
				W := s.getNumWindowsCovering(ingestBuffer)
				bs := int64(0)
				be := int64(0)

				for w := W - 1; w >= 0; w -= 1 {
					be = bs + s.windowLengths[w] - 1
					ts, okStart := ingestBuffer.GetTimestamp(bs)
					te, okEnd := ingestBuffer.GetTimestamp(be)

					if !(okStart && okEnd) {
						continue
					}

					window := NewSummaryWindow(
						ts, te, s.numElements+bs, s.numElements+be)

					for c := bs; c <= be; c += 1 {
						timestamp, okt := ingestBuffer.GetTimestamp(c)
						value, okv := ingestBuffer.GetValue(c)
						if !(okt && okv) {
							continue
						}
						s.streamWindowManager.InsertIntoSummaryWindow(window, timestamp, value)
					}

					writerQueue <- window
					bs = be + 1
				}
				s.numElements += bs

				if bs == ingestBuffer.Size {
					ingestBuffer.Clear()
					emptyBuffers <- ingestBuffer
				} else {
					ingestBuffer.TruncateHead(bs)
					partialBuffers <- ingestBuffer
				}
			}
		case <-ctx.Done():
			return
		}
	}
}
