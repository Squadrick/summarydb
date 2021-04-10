package core

import (
	"context"
)

// Writer doesn't have much logic, it gets a stream of SummaryWindow
// which are sequentially written to disk using StreamWindowManager.
// Once a window is successfully written to disk, it sends a MergeEvent
// to the Merger with info of just written window.
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

func (w *Writer) flush() {
	if w.barrier != nil {
		w.barrier.Notify(WRITER)
	}
}

func (w *Writer) Run(ctx context.Context, inputCh <-chan *SummaryWindow, outputCh chan<- *MergeEvent) {
	for {
		select {

		case summaryWindow := <-inputCh:
			if summaryWindow == ConstShutdownSummaryWindow() {
				w.flush()
				return
			} else if summaryWindow == ConstFlushSummaryWindow() {
				w.flush()
				continue
			} else {
				w.streamWindowManager.PutSummaryWindow(summaryWindow)
				mergerEvent := &MergeEvent{
					Id:   summaryWindow.TimeStart,
					Size: summaryWindow.CountEnd - summaryWindow.CountStart + 1,
				}
				outputCh <- mergerEvent
			}
		case <-ctx.Done():
			return
		}
	}
}
