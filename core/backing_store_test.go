package core

import (
	"github.com/google/go-cmp/cmp"
	"summarydb/storage"
	"summarydb/utils"
	"testing"
)

func GetSummaryWindow() *SummaryWindow {
	window := NewSummaryWindow(1, 2, 3, 4)
	window.Data.Max.Value = 12.12
	window.Data.Count.Value = 13.13
	window.Data.Count.Value = 14.14
	return window
}

func GetLandmarkWindow() *LandmarkWindow {
	window := NewLandmarkWindow(3)
	window.Insert(4, 1.2)
	window.Insert(5, 1.6)
	window.Insert(6, 2.0)
	window.Close(10)
	return window
}

func TestSummaryWindowSerialization(t *testing.T) {
	window := GetSummaryWindow()
	buf := SummaryWindowToBytes(window)
	newWindow := BytesToSummaryWindow(buf)

	utils.AssertTrue(t, cmp.Equal(window, newWindow))
}

func TestLandmarkWindowSerialization(t *testing.T) {
	window := GetLandmarkWindow()
	buf := LandmarkWindowToBytes(window)
	newWindow := BytesToLandmarkWindow(buf)

	utils.AssertTrue(t, cmp.Equal(window, newWindow))
}

func TestInMemory(t *testing.T) {
	summaryWindow := GetSummaryWindow()
	landmarkWindow := GetLandmarkWindow()
	backend := storage.NewInMemoryBackend()
	store := NewBackingStore(backend)

	store.Put(0, 1, summaryWindow)
	store.PutLandmark(1, 1, landmarkWindow)

	newSummaryWindow := store.Get(0, 1)
	newLandmarkWindow := store.GetLandmark(1, 1)

	utils.AssertTrue(t, cmp.Equal(summaryWindow, newSummaryWindow))
	utils.AssertTrue(t, cmp.Equal(landmarkWindow, newLandmarkWindow))
}