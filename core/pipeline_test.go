package core

import (
	"context"
	"github.com/stretchr/testify/assert"
	"summarydb/storage"
	"summarydb/window"
	"testing"
	"time"
)

var ExpectedEvolution = [][]int64{
	{1},
	{1, 1},
	{2, 1},
	{2, 1, 1},
	{2, 2, 1},
	{2, 2, 1, 1},
	{2, 2, 2, 1}, // {4, 2, 1}
	{4, 2, 1, 1},
	{4, 2, 2, 1},
	{4, 2, 2, 1, 1},
	{4, 2, 2, 2, 1}, // {4, 4, 2, 1}
	{4, 4, 2, 1, 1},
	{4, 4, 2, 2, 1},
	{4, 4, 2, 2, 1, 1},
	{4, 4, 2, 2, 2, 1}, // {8, 4, 2, 1}
	{4, 4, 4, 2, 1, 1},
}

func TestPipeline_Run_Unbuffered(t *testing.T) {
	manager := NewStreamWindowManager(0, []string{"count"})
	backend := storage.NewInMemoryBackend()
	manager.SetBackingStore(NewBackingStore(backend))
	windowing := window.NewGenericWindowing(window.NewExponentialLengthsSequence(2))
	pipeline := NewPipeline(windowing)
	pipeline.SetWindowManager(manager)

	for ti := int64(0); ti < int64(len(ExpectedEvolution)); ti += 1 {
		pipeline.Append(ti, 0)
		time.Sleep(50 * time.Millisecond)
		expectedAnswer := ExpectedEvolution[ti]
		results := make([]int64, 0)
		summaryWindows := manager.GetSummaryWindowInRange(0, ti)

		for _, summaryWindow := range summaryWindows {
			results = append(results, int64(summaryWindow.Data.Count.Value))
		}

		assert.Equal(t, expectedAnswer, results)
	}
}

func TestPipeline_Run_Buffered(t *testing.T) {
	manager := NewStreamWindowManager(0, []string{"count"})
	backend := storage.NewInMemoryBackend()
	manager.SetBackingStore(NewBackingStore(backend))
	windowing := window.NewGenericWindowing(window.NewExponentialLengthsSequence(2))
	pipeline := NewPipeline(windowing)
	pipeline.SetBufferSize(4, 4)
	pipeline.SetWindowsPerBatch(2)
	pipeline.SetWindowManager(manager)

	ctx := context.Background()
	cancelCtx, cancelFunc := context.WithCancel(ctx)
	pipeline.Run(cancelCtx)

	for ti := int64(0); ti < int64(len(ExpectedEvolution)); ti += 1 {
		pipeline.Append(ti, 0)
		pipeline.Flush(false, false)
		time.Sleep(50 * time.Millisecond)
		expectedAnswer := ExpectedEvolution[ti]
		results := make([]int64, 0)
		summaryWindows := manager.GetSummaryWindowInRange(0, ti)

		for _, summaryWindow := range summaryWindows {
			results = append(results, int64(summaryWindow.Data.Count.Value))
		}

		if ti%2 == 0 {
			assert.Equal(t, expectedAnswer, results)
		}
	}
	cancelFunc()
}

func TestPipeline_Run(t *testing.T) {
	manager := NewStreamWindowManager(0, []string{"count"})
	backend := storage.NewInMemoryBackend()
	manager.SetBackingStore(NewBackingStore(backend))
	windowing := window.NewGenericWindowing(window.NewExponentialLengthsSequence(2))
	pipeline := NewPipeline(windowing)
	pipeline.SetBufferSize(8, 4)
	pipeline.SetWindowsPerBatch(2)
	pipeline.SetWindowManager(manager)

	ctx := context.Background()
	cancelCtx, cancelFunc := context.WithCancel(ctx)
	pipeline.Run(cancelCtx)

	for ti := int64(0); ti < int64(len(ExpectedEvolution)); ti += 1 {
		pipeline.Append(ti, 0)
	}

	pipeline.Flush(true, true)
	time.Sleep(50 * time.Millisecond)
	tl := int64(len(ExpectedEvolution) - 1)
	expectedAnswer := ExpectedEvolution[tl]
	results := make([]int64, 0)
	summaryWindows := manager.GetSummaryWindowInRange(0, tl)

	for _, summaryWindow := range summaryWindows {
		results = append(results, int64(summaryWindow.Data.Count.Value))
	}
	assert.Equal(t, expectedAnswer, results)
	cancelFunc()
}
