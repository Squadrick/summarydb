package core

import (
	"context"
	"summarydb/window"
	"testing"
)

func TestHeapMerger_Run(t *testing.T) {
	ctx, cancelFunc := context.WithCancel(context.Background())

	heapMerger := NewHeapMerger(window.NewGenericWindowing(window.NewExponentialLengthsSequence(1.2)), 10)

	ch := make(chan *window.Info, 10)
	go heapMerger.Run(ctx, ch)

	cancelFunc()
	close(ch)
}
