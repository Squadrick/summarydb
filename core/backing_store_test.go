package core

import (
	cmp "github.com/google/go-cmp/cmp"
	"summarydb/utils"
	"testing"
)

func TestSummaryWindowSerialization(t *testing.T) {
	window := NewSummaryWindow(1, 2, 3, 4)
	window.Data.Max.Value = 12.12
	window.Data.Count.Value = 13.13
	window.Data.Count.Value = 14.14

	buf := SummaryWindowToBytes(window)
	newWindow := BytesToSummaryWindow(buf)

	utils.AssertTrue(t, cmp.Equal(window, newWindow))
}

func TestLandmarkWindowSerialization(t *testing.T) {
	window := NewLandmarkWindow(3)
	window.Insert(4, 1.2)
	window.Insert(5, 1.6)
	window.Insert(6, 2.0)
	window.Close(10)

	buf := LandmarkWindowToBytes(window)
	newWindow := BytesToLandmarkWindow(buf)

	utils.AssertTrue(t, cmp.Equal(window, newWindow))
}
