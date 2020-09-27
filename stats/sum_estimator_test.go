package stats

import (
	"summarydb/core"
	"summarydb/utils"
	"testing"
)

func getValue(table *core.DataTable) float64 {
	return table.Count.Value
}

func TestGetSumStats_NoLandmarks(t *testing.T) {
	summaryWindows := make([]core.SummaryWindow, 0)
	landmarkWindows := make([]core.LandmarkWindow, 0)

	for i := int64(0); i < 5; i++ {
		summaryWindow := core.NewSummaryWindow(i*5, (i+1)*5-1, i, i+1)
		summaryWindow.Data.Count.Value = 1
		summaryWindows = append(summaryWindows, *summaryWindow)
	}

	bounds, stats := GetSumStats(3, 21, summaryWindows, landmarkWindows, getValue)

	utils.AssertEqual(t, bounds.Lower, 3.0)
	utils.AssertEqual(t, bounds.Upper, 5.0)
	utils.AssertEqual(t, stats.Mean, 3.8)
	utils.AssertEqual(t, stats.Var, 0.48)
}

func TestGetSumStats_Landmarks(t *testing.T) {
	summaryWindows := make([]core.SummaryWindow, 0)
	landmarkWindows := make([]core.LandmarkWindow, 0)

	for i := int64(0); i < 5; i++ {
		if i == 2 {
			continue
		}
		summaryWindow := core.NewSummaryWindow(i*5, (i+1)*5-1, i, i+1)
		summaryWindow.Data.Count.Value = 1
		summaryWindows = append(summaryWindows, *summaryWindow)
	}

	landmarkData := core.NewDataTable()
	landmarkData.Count.Value = 1

	landmarkWindow1 := core.NewLandmarkWindow(2)
	landmarkWindow1.Insert(3, landmarkData)
	landmarkWindow1.Close(5)

	landmarkWindow2 := core.NewLandmarkWindow(5)
	landmarkWindow2.Insert(6, landmarkData)
	landmarkWindow2.Insert(7, landmarkData)
	landmarkWindow2.Insert(8, landmarkData)
	landmarkWindow2.Close(9)

	landmarkWindows = append(landmarkWindows, *landmarkWindow1, *landmarkWindow2)

	bounds, stats := GetSumStats(1, 21, summaryWindows, landmarkWindows, getValue)

	utils.AssertEqual(t, bounds.Lower, 6.0)
	utils.AssertEqual(t, bounds.Upper, 8.0)
	utils.AssertEqual(t, stats.Mean, 6.9)
	utils.AssertEqual(t, stats.Var, 0.49)
}
