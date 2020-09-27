package core

import (
	"summarydb/utils"
	"testing"
)

func TestSummaryWindowSerialization(t *testing.T) {
	window := NewSummaryWindow(1, 2, 3, 4)
	window.Data.Max.Value = 12
	window.Data.Count.Value = 13
	window.Data.Count.Value = 14

	buf := SummaryWindowToBytes(window)

	newWindow := BytesToSummaryWindow(buf)

	utils.AssertEqual(t, window.TimeStart, newWindow.TimeStart)
	utils.AssertEqual(t, window.TimeEnd, newWindow.TimeEnd)
	utils.AssertEqual(t, window.CountStart, newWindow.CountStart)
	utils.AssertEqual(t, window.CountEnd, newWindow.CountEnd)
	utils.AssertEqual(t, window.Data.Count.Value, newWindow.Data.Count.Value)
	utils.AssertEqual(t, window.Data.Sum.Value, newWindow.Data.Sum.Value)
	utils.AssertEqual(t, window.Data.Max.Value, newWindow.Data.Max.Value)
}
