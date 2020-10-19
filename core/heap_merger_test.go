package core

import (
	"context"
	"summarydb/window"
	"testing"
	"time"
)

func TestHeapMerger_Run(t *testing.T) {
	ctx, cancelFunc := context.WithCancel(context.Background())

	heapMerger := NewHeapMerger(window.NewGenericWindowing(window.NewExponentialLengthsSequence(1.2)), 10)

	ch := make(chan *MergeEvent, 10)
	go heapMerger.Run(ctx, ch)

	for i := 0; i < 100; i++ {
		info := &MergeEvent{
			Id:   int64(i),
			Size: int64(i),
		}
		ch <- info
	}
	time.Sleep(1 * time.Second)
	cancelFunc()
	close(ch)
}
