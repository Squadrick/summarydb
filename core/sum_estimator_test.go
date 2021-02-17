package core

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func getValue(table *DataTable) float64 {
	return table.Count.Value
}

func TestGetSumStats_NoLandmarks(t *testing.T) {
	summaryWindows := make([]*SummaryWindow, 0)
	landmarkWindows := make([]*LandmarkWindow, 0)

	for i := int64(0); i < 5; i++ {
		summaryWindow := NewSummaryWindow(i*5, (i+1)*5-1, i, i+1)
		summaryWindow.Data.Count.Value = 1
		summaryWindows = append(summaryWindows, summaryWindow)
	}

	bounds, stats := GetSumStats(3, 21, summaryWindows, landmarkWindows, getValue)

	assert.Equal(t, bounds.Lower, 3.0)
	assert.Equal(t, bounds.Upper, 5.0)
	assert.Equal(t, stats.Mean, 3.8)
	assert.Equal(t, stats.Var, 0.48)
}

func TestGetSumStats_Landmarks(t *testing.T) {
	summaryWindows := make([]*SummaryWindow, 0)
	landmarkWindows := make([]*LandmarkWindow, 0)

	for i := int64(0); i < 5; i++ {
		if i == 2 {
			continue
		}
		summaryWindow := NewSummaryWindow(i*5, (i+1)*5-1, i, i+1)
		summaryWindow.Data.Count.Value = 1
		summaryWindows = append(summaryWindows, summaryWindow)
	}

	landmarkWindow1 := NewLandmarkWindow(2)
	landmarkWindow1.Insert(3, 1.0)
	landmarkWindow1.Close(5)

	landmarkWindow2 := NewLandmarkWindow(5)
	landmarkWindow2.Insert(6, 1.0)
	landmarkWindow2.Insert(7, 1.0)
	landmarkWindow2.Insert(8, 1.0)
	landmarkWindow2.Close(9)

	landmarkWindows = append(landmarkWindows, landmarkWindow1, landmarkWindow2)

	bounds, stats := GetSumStats(1, 21, summaryWindows, landmarkWindows, getValue)

	assert.Equal(t, bounds.Lower, 6.0)
	assert.Equal(t, bounds.Upper, 8.0)
	assert.Equal(t, stats.Mean, 6.9)
	assert.Equal(t, stats.Var, 0.49)
}
