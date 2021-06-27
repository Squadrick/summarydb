package core

import (
	"context"
	"errors"
	"summarydb/storage"
)

// Writer doesn't have much logic, it gets a stream of SummaryWindow
// which are sequentially written to disk using StreamWindowManager.
// Once a window is successfully written to disk, it sends a MergeEvent
// to the Merger with info of just written window.
type Writer struct {
	streamWindowManager *StreamWindowManager
	barrier             *Barrier
	numElements         int64
	latestTimeStart     int64
}

func NewWriter(barrier *Barrier) *Writer {
	return &Writer{
		streamWindowManager: nil,
		barrier:             barrier,
		numElements:         0,
		latestTimeStart:     0,
	}
}

func (w *Writer) SetWindowManager(manager *StreamWindowManager) {
	w.streamWindowManager = manager
}

func (w *Writer) PrimeUp() error {
	if w.streamWindowManager == nil {
		return errors.New("cannot prime without window manager")
	}
	numElements, ts, err := w.streamWindowManager.GetCountAndTime(storage.Writer)
	if err != nil {
		return err
	}
	w.numElements = numElements
	w.latestTimeStart = ts
	return nil

}

func (w *Writer) flush() {
	if w.barrier != nil {
		w.barrier.Notify(WRITER)
	}
}

func (w *Writer) Process(summaryWindow *SummaryWindow) (*MergeEvent, error) {
	size := summaryWindow.Size()
	w.numElements += size
	w.latestTimeStart = summaryWindow.TimeStart
	err := w.streamWindowManager.WriterBrew(
		w.numElements, w.latestTimeStart, summaryWindow)
	if err != nil {
		return nil, err
	}
	mergerEvent := &MergeEvent{
		Id:   summaryWindow.Id(),
		Size: size,
	}
	return mergerEvent, nil
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
				mergerEvent, err := w.Process(summaryWindow)
				if err != nil {
					panic(err)
				}
				outputCh <- mergerEvent
			}
		case <-ctx.Done():
			return
		}
	}
}
