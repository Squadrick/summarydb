package core

import (
	"context"
	"github.com/stretchr/testify/assert"
	"strconv"
	"summarydb/storage"
	"summarydb/window"
	"testing"
)

// 31 time steps
var ExpectedEvolution = [][]int64{
	{1},
	{1, 1},
	{2, 1},
	{2, 1, 1},
	{2, 2, 1},
	{2, 2, 1, 1},
	{4, 2, 1},
	{4, 2, 1, 1},
	{4, 2, 2, 1},
	{4, 2, 2, 1, 1},
	{4, 4, 2, 1},
	{4, 4, 2, 1, 1},
	{4, 4, 2, 2, 1},
	{4, 4, 2, 2, 1, 1},
	{8, 4, 2, 1},
	{8, 4, 2, 1, 1},
	{8, 4, 2, 2, 1},
	{8, 4, 2, 2, 1, 1},
	{8, 4, 4, 2, 1},
	{8, 4, 4, 2, 1, 1},
	{8, 4, 4, 2, 2, 1},
	{8, 4, 4, 2, 2, 1, 1},
	{8, 8, 4, 2, 1},
	{8, 8, 4, 2, 1, 1},
	{8, 8, 4, 2, 2, 1},
	{8, 8, 4, 2, 2, 1, 1},
	{8, 8, 4, 4, 2, 1},
	{8, 8, 4, 4, 2, 1, 1},
	{8, 8, 4, 4, 2, 2, 1},
	{8, 8, 4, 4, 2, 2, 1, 1},
	{16, 8, 4, 2, 1},
}

func TestPipeline_EachStep_Unbuffered(t *testing.T) {
	manager := NewStreamWindowManager(0, []string{"count"})
	backend := storage.NewInMemoryBackend()
	manager.SetBackingStore(NewBackingStore(backend, false))
	windowing := window.NewGenericWindowing(window.NewExponentialLengthsSequence(2))
	pipeline := NewPipeline(windowing).
		SetWindowManager(manager).
		SetUnbuffered()

	for ti := int64(0); ti < int64(len(ExpectedEvolution)); ti += 1 {
		pipeline.Append(ti, 0)
		expectedAnswer := ExpectedEvolution[ti]
		results := make([]int64, 0)
		summaryWindows := manager.GetSummaryWindowInRange(0, ti)

		for _, summaryWindow := range summaryWindows {
			results = append(results, int64(summaryWindow.Data.Count.Value))
		}
		assert.Equal(t, expectedAnswer, results)
	}
}

func TestPipeline_EachStep_Buffered(t *testing.T) {
	manager := NewStreamWindowManager(0, []string{"count"})
	backend := storage.NewInMemoryBackend()
	manager.SetBackingStore(NewBackingStore(backend, false))
	windowing := window.NewGenericWindowing(window.NewExponentialLengthsSequence(2))
	pipeline := NewPipeline(windowing).
		SetBufferSize(31 /*4 windows*/).
		SetWindowsPerMerge(2).
		SetWindowManager(manager)

	ctx := context.Background()
	cancelCtx, cancelFunc := context.WithCancel(ctx)
	pipeline.Run(cancelCtx)

	for ti := int64(0); ti < int64(len(ExpectedEvolution)); ti += 1 {
		pipeline.Append(ti, 0)
		pipeline.Flush(false)
		//time.Sleep(50 * time.Millisecond)
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

func testPipelineFinalStep(t *testing.T, backend storage.Backend) {
	manager := NewStreamWindowManager(0, []string{"count"})
	manager.SetBackingStore(NewBackingStore(backend, false))
	windowing := window.NewGenericWindowing(window.NewExponentialLengthsSequence(2))
	pipeline := NewPipeline(windowing).
		SetBufferSize(50).
		SetWindowsPerMerge(2).
		SetWindowManager(manager)

	ctx := context.Background()
	cancelCtx, cancelFunc := context.WithCancel(ctx)
	pipeline.Run(cancelCtx)

	for ti := int64(0); ti < int64(len(ExpectedEvolution)); ti += 1 {
		pipeline.Append(ti, 0)
	}

	pipeline.Flush(true)
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

func TestPipeline_Run_Memory(t *testing.T) {
	backend := storage.NewInMemoryBackend()
	testPipelineFinalStep(t, backend)
}

func TestPipeline_Run_Badger(t *testing.T) {
	backend := storage.NewBadgerBacked(storage.TestBadgerBackendConfig())
	testPipelineFinalStep(t, backend)
}

// Benchmarks here

func benchmarkPipeline(b *testing.B,
	windowing window.Windowing,
	totalBufferSize int64,
	numBuffers int64,
	windowsPerBatch int64) {
	manager := NewStreamWindowManager(0, []string{"count"})
	backend := storage.NewBadgerBacked(storage.TestBadgerBackendConfig())
	manager.SetBackingStore(NewBackingStore(backend, true))
	pipeline := NewPipeline(windowing).
		SetBufferSize(totalBufferSize).
		SetWindowsPerMerge(windowsPerBatch).
		SetWindowManager(manager)
	pipeline.SetWindowManager(manager)

	ctx := context.Background()
	cancelCtx, cancelFunc := context.WithCancel(ctx)
	pipeline.Run(cancelCtx)

	for n := 0; n < b.N; n++ {
		pipeline.Append(int64(n), float64(n))
	}

	pipeline.Flush(true)
	cancelFunc()
}

func benchmarkPipelineLoop(b *testing.B, windowing window.Windowing) {
	limit := 4
	for totalBufferSize := 4; totalBufferSize <= 64; totalBufferSize *= 4 {
		for numBuffer := 1; numBuffer <= limit; numBuffer *= 2 {
			for windowsPerBatch := 1; windowsPerBatch <= limit; windowsPerBatch *= 2 {
				name := strconv.Itoa(totalBufferSize) + "/" + strconv.Itoa(numBuffer) + "/" + strconv.Itoa(windowsPerBatch)
				b.Run(name, func(b *testing.B) {
					// window size = totalBufferSize / numBuffer
					benchmarkPipeline(b,
						windowing,
						int64(totalBufferSize),
						int64(numBuffer),
						int64(windowsPerBatch))
				})
			}
		}
	}
}

func BenchmarkPipeline_Exp(b *testing.B) {
	windowing := window.NewGenericWindowing(window.NewExponentialLengthsSequence(2))
	benchmarkPipelineLoop(b, windowing)
}

func BenchmarkPipeline_Power(b *testing.B) {
	// root N growth
	windowing := window.NewPowerWindowing(1, 1, 10, 1)
	benchmarkPipelineLoop(b, windowing)
}
