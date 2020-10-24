package core

import (
	"context"
)

type Writer struct {
	streamWindowManager *StreamWindowManager
	barrier             *Barrier
}

func NewWriter(barrier *Barrier) *Writer {
	return &Writer{
		streamWindowManager: nil,
		barrier:             barrier,
	}
}

func (w *Writer) SetWindowManager(manager *StreamWindowManager) {
	w.streamWindowManager = manager
}

func (w *Writer) Run(ctx context.Context, inputCh <-chan *SummaryWindow, outputCh chan<- *MergeEvent) {
	for {
		select {

		case summaryWindow := <-inputCh:
			if summaryWindow == ConstShutdownSummaryWindow() {
				if w.barrier != nil {
					w.barrier.Notify(WRITER)
				}
				return
			} else if summaryWindow == ConstFlushSummaryWindow() {
				if w.barrier != nil {
					w.barrier.Notify(WRITER)
				}
				continue
			} else {
				w.streamWindowManager.PutSummaryWindow(summaryWindow)
				info := &MergeEvent{
					Id:   summaryWindow.TimeStart,
					Size: summaryWindow.CountEnd - summaryWindow.CountStart + 1,
				}
				outputCh <- info
			}
		case <-ctx.Done():
			return
		}
	}
}
