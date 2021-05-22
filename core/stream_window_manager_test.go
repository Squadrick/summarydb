package core

import (
	"github.com/stretchr/testify/assert"
	"summarydb/storage"
	"testing"
)

func testStreamWindowManager(t *testing.T, backend storage.Backend) {
	store := NewBackingStore(backend, false)

	manager := NewStreamWindowManager(0, []string{"max", "sum", "count"})
	manager.SetBackingStore(store)

	for i := int64(0); i < 5; i++ {
		summaryWindow := NewSummaryWindow(i*5, (i+1)*5-1, i, i+1)
		manager.InsertIntoSummaryWindow(summaryWindow, i*5, float64(i))
		manager.PutSummaryWindow(summaryWindow)
	}
	assert.Equal(t, manager.NumSummaryWindows(), 5)

	for i := int64(0); i < 3; i++ {
		landmarkWindow := NewLandmarkWindow(3 * i)
		landmarkWindow.Insert(3*i+1, float64(i))
		landmarkWindow.Close(3*i + 2)
		manager.PutLandmarkWindow(landmarkWindow)
	}
	assert.Equal(t, manager.NumLandmarkWindows(), 3)

	middleSummaryWindows := manager.GetSummaryWindowInRange(6, 16)
	assert.Equal(t, len(middleSummaryWindows), 3)

	for _, m := range middleSummaryWindows {
		assert.True(t, m.TimeEnd > 5)
		assert.True(t, m.TimeEnd < 20)
		manager.DeleteSummaryWindow(m.TimeStart)
	}
	assert.Equal(t, manager.NumSummaryWindows(), 2)

	middleLandmarkWindows := manager.GetLandmarkWindowInRange(1, 3)
	assert.Equal(t, len(middleLandmarkWindows), 2)
	for _, m := range middleLandmarkWindows {
		manager.DeleteLandmarkWindow(m.TimeStart)
	}
	assert.Equal(t, manager.NumLandmarkWindows(), 1)
}

func TestStreamWindowManager_InMemory(t *testing.T) {
	backend := storage.NewInMemoryBackend()
	defer backend.Close()
	testStreamWindowManager(t, backend)
}

func TestStreamWindowManager_Badger(t *testing.T) {
	config := storage.TestBadgerDB()
	backend := storage.NewBadgerBacked(config)
	defer backend.Close()
	testStreamWindowManager(t, backend)
}

func testStreamWindowManagerMerge(t *testing.T, backend storage.Backend) {
	store := NewBackingStore(backend, false)

	manager := NewStreamWindowManager(0, []string{"max", "sum", "count"})
	manager.SetBackingStore(store)

	for i := int64(0); i < 5; i++ {
		summaryWindow := NewSummaryWindow(i*5, (i+1)*5-1, i, i+1)
		manager.InsertIntoSummaryWindow(summaryWindow, i*5, float64(2*i+1))
		manager.PutSummaryWindow(summaryWindow)
	}
	middleSummaryWindows := manager.GetSummaryWindowInRange(1, 23)
	mergedWindow := manager.MergeSummaryWindows(middleSummaryWindows)

	assert.Equal(t, mergedWindow.TimeEnd, int64(24))
	assert.Equal(t, mergedWindow.Data.Count.Value, float64(5))
	assert.Equal(t, mergedWindow.Data.Max.Value, float64(9))
	assert.Equal(t, mergedWindow.Data.Sum.Value, float64(25))
}

func TestStreamWindowManagerMerge_InMemory(t *testing.T) {
	backend := storage.NewInMemoryBackend()
	defer backend.Close()
	testStreamWindowManagerMerge(t, backend)
}

func TestStreamWindowManagerMerge_Badger(t *testing.T) {
	config := storage.TestBadgerDB()
	backend := storage.NewBadgerBacked(config)
	defer backend.Close()
	testStreamWindowManagerMerge(t, backend)
}
