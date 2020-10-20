package core

import (
	"github.com/stretchr/testify/assert"
	"summarydb/storage"
	"summarydb/window"
	"testing"
)

func TestPipeline_Run_Unbuffered(t *testing.T) {
	manager := NewStreamWindowManager(0, []string{"count"})
	backend := storage.NewInMemoryBackend()
	manager.SetBackingStore(NewBackingStore(backend))
	windowing := window.NewGenericWindowing(window.NewExponentialLengthsSequence(2))
	pipeline := NewPipeline(windowing)
	pipeline.SetWindowManager(manager)

	expectedEvolution := [][]int64{
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
	}

	for ti := int64(0); ti < int64(len(expectedEvolution)); ti += 1 {
		pipeline.Append(ti, 0)
		expectedAnswer := expectedEvolution[ti]
		results := make([]int64, 0)
		summaryWindows := manager.GetSummaryWindowInRange(0, ti)

		for _, summaryWindow := range summaryWindows {
			results = append(results, int64(summaryWindow.Data.Count.Value))
		}

		assert.Equal(t, expectedAnswer, results)
	}
}
