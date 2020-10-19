package core

import (
	"context"
	"fmt"
)

type Writer struct {
	streamWindowManager *StreamWindowManager
}

func NewWriter() *Writer {
	return &Writer{streamWindowManager: nil}
}

func (w *Writer) SetWindowManager(manager *StreamWindowManager) {
	w.streamWindowManager = manager
}

func (w *Writer) Run(ctx context.Context, inputCh <-chan *SummaryWindow, outputCh chan<- *MergeEvent) {
	for {
		select {

		case summaryWindow := <-inputCh:
			if summaryWindow == ConstShutdownSummaryWindow() {
				// notify(WRITER)
				fmt.Println("showdown")
				return
			} else if summaryWindow == ConstFlushSummaryWindow() {
				// notify(WRITER)
				fmt.Println("flush")
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
			fmt.Println("ctx cancelled")
			return
		}
	}
}
