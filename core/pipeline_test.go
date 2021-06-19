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
var ExpectedEvolutionExp = [][]int64{
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

// 22 time steps
var ExpectedEvolutionPower = [][]int64{
	{1},
	{1, 1},
	{1, 1, 1},
	{1, 1, 1, 1},
	{1, 1, 1, 1, 1},
	{2, 1, 1, 1, 1},
	{2, 1, 1, 1, 1, 1},
	{2, 2, 1, 1, 1, 1},
	{2, 2, 1, 1, 1, 1, 1},
	{2, 2, 2, 1, 1, 1, 1},
	{2, 2, 2, 1, 1, 1, 1, 1},
	{2, 2, 2, 2, 1, 1, 1, 1},
	{2, 2, 2, 2, 1, 1, 1, 1, 1},
	{2, 2, 2, 2, 2, 1, 1, 1, 1},
	{2, 2, 2, 2, 2, 1, 1, 1, 1, 1},
	{2, 2, 2, 2, 2, 2, 1, 1, 1, 1},
	{2, 2, 2, 2, 2, 2, 1, 1, 1, 1, 1},
	{2, 2, 2, 2, 2, 2, 2, 1, 1, 1, 1},
	{2, 2, 2, 2, 2, 2, 2, 1, 1, 1, 1, 1},
	{2, 2, 2, 2, 2, 2, 2, 2, 1, 1, 1, 1},
	{2, 2, 2, 2, 2, 2, 2, 2, 1, 1, 1, 1, 1},
	{2, 2, 2, 2, 2, 2, 2, 2, 2, 1, 1, 1, 1},
}

func testPipeline_EachStep_Unbuffered(t *testing.T,
	windowing window.Windowing,
	expectedEvolution [][]int64) {
	manager := NewStreamWindowManager(0, []string{"count"})
	backend := storage.NewInMemoryBackend()
	manager.SetBackingStore(NewBackingStore(backend, false))
	pipeline := NewPipeline(windowing).
		SetWindowManager(manager).
		SetUnbuffered()

	for ti := int64(0); ti < int64(len(expectedEvolution)); ti += 1 {
		err := pipeline.Append(ti, 0)
		assert.NoError(t, err)
		expectedAnswer := expectedEvolution[ti]
		results := make([]int64, 0)
		summaryWindows, err := manager.GetSummaryWindowInRange(0, ti)
		assert.NoError(t, err)

		for _, summaryWindow := range summaryWindows {
			results = append(results, int64(summaryWindow.Data.Count.Value))
		}
		assert.Equal(t, expectedAnswer, results)
	}
}

func TestPipeline_EachStep_Unbuffered_Exp(t *testing.T) {
	windowing := window.NewGenericWindowing(window.NewExponentialLengthsSequence(2))
	testPipeline_EachStep_Unbuffered(t, windowing, ExpectedEvolutionExp)
}

func TestPipeline_EachStep_Unbuffered_Power(t *testing.T) {
	windowing := window.NewGenericWindowing(window.NewPowerLengthsSequence(1, 1, 4, 1))
	print(len(ExpectedEvolutionPower))
	testPipeline_EachStep_Unbuffered(t, windowing, ExpectedEvolutionPower)
}

func testPipeline_EachStep_Buffered(t *testing.T,
	windowing window.Windowing,
	expectedEvolution [][]int64) {
	manager := NewStreamWindowManager(0, []string{"count"})
	backend := storage.NewInMemoryBackend()
	manager.SetBackingStore(NewBackingStore(backend, false))
	pipeline := NewPipeline(windowing).
		SetBufferSize(31 /*4 windows*/).
		SetWindowsPerMerge(2).
		SetWindowManager(manager)

	ctx := context.Background()
	cancelCtx, cancelFunc := context.WithCancel(ctx)
	pipeline.Run(cancelCtx)

	for ti := int64(0); ti < int64(len(expectedEvolution)); ti += 1 {
		err := pipeline.Append(ti, 0)
		assert.NoError(t, err)
		err = pipeline.Flush(false)
		assert.NoError(t, err)
		expectedAnswer := expectedEvolution[ti]
		results := make([]int64, 0)
		summaryWindows, err := manager.GetSummaryWindowInRange(0, ti)
		assert.NoError(t, err)

		for _, summaryWindow := range summaryWindows {
			results = append(results, int64(summaryWindow.Data.Count.Value))
		}

		if ti%2 == 0 {
			assert.Equal(t, expectedAnswer, results)
		}
	}
	cancelFunc()
}

func TestPipeline_EachStep_Buffered_Exp(t *testing.T) {
	windowing := window.NewGenericWindowing(window.NewExponentialLengthsSequence(2))
	testPipeline_EachStep_Buffered(t, windowing, ExpectedEvolutionExp)
}

func TestPipeline_EachStep_Buffered_Power(t *testing.T) {
	windowing := window.NewGenericWindowing(window.NewPowerLengthsSequence(1, 1, 4, 1))
	testPipeline_EachStep_Buffered(t, windowing, ExpectedEvolutionPower)
}

func testPipelineFinalStep(t *testing.T, backend storage.Backend) {
	manager := NewStreamWindowManager(0, []string{"count"})
	manager.SetBackingStore(NewBackingStore(backend, false))
	windowing := window.NewGenericWindowing(window.NewExponentialLengthsSequence(2))
	pipeline := NewPipeline(windowing).
		SetBufferSize(8).
		SetWindowsPerMerge(2).
		SetWindowManager(manager)

	ctx := context.Background()
	cancelCtx, cancelFunc := context.WithCancel(ctx)
	pipeline.Run(cancelCtx)

	for ti := int64(0); ti < int64(len(ExpectedEvolutionExp)); ti += 1 {
		err := pipeline.Append(ti, 0)
		assert.NoError(t, err)
	}

	err := pipeline.Flush(true)
	assert.NoError(t, err)
	tl := int64(len(ExpectedEvolutionExp) - 1)
	results := make([]int64, 0)
	summaryWindows, err := manager.GetSummaryWindowInRange(0, tl)
	assert.NoError(t, err)
	for _, summaryWindow := range summaryWindows {
		results = append(results, int64(summaryWindow.Data.Count.Value))
	}

	reduceSum := func(arr []int64) int64 {
		sum := int64(0)
		for i := 0; i < len(arr); i++ {
			sum += arr[i]
		}
		return sum
	}
	assert.Equal(t, tl+1, reduceSum(results))
	cancelFunc()
}

func TestPipeline_Run_Memory(t *testing.T) {
	backend := storage.NewInMemoryBackend()
	testPipelineFinalStep(t, backend)
}

func TestPipeline_Run_Badger(t *testing.T) {
	backend := storage.NewBadgerBacked(storage.TestBadgerDB())
	testPipelineFinalStep(t, backend)
}

// Benchmarks here

func benchmarkPipeline(b *testing.B,
	windowing window.Windowing,
	totalBufferSize int64,
	numBuffers int64,
	windowsPerBatch int64) {
	manager := NewStreamWindowManager(0, []string{"count"})
	backend := storage.NewBadgerBacked(storage.TestBadgerDB())
	manager.SetBackingStore(NewBackingStore(backend, true))
	pipeline := NewPipeline(windowing).
		SetBufferSize(totalBufferSize).
		SetWindowsPerMerge(windowsPerBatch).
		SetWindowManager(manager).
		SetNumBuffers(numBuffers)
	pipeline.SetWindowManager(manager)

	ctx := context.Background()
	cancelCtx, cancelFunc := context.WithCancel(ctx)
	pipeline.Run(cancelCtx)

	for n := 0; n < b.N; n++ {
		err := pipeline.Append(int64(n), float64(n))
		if err != nil {
			b.FailNow()
		}
	}

	err := pipeline.Flush(true)
	if err != nil {
		b.FailNow()
	}
	cancelFunc()
}

func benchmarkPipelineLoop(b *testing.B, windowing window.Windowing) {
	limit := 4
	for totalBufferSize := 8; totalBufferSize <= 128; totalBufferSize *= 4 {
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

func BenchmarkPipeline_PowerSeq(b *testing.B) {
	// root N growth
	windowing := window.NewGenericWindowing(window.NewPowerLengthsSequence(1, 1, 10, 1))
	benchmarkPipelineLoop(b, windowing)
}
