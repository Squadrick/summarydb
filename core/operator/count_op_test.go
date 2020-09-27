package operator

import (
	"summarydb/core"
	"summarydb/utils"
	"testing"
)

func TestCountOp_Apply(t *testing.T) {
	data := core.NewDataTable()
	data.Count.Value = 3

	op := NewCountOp()
	op.Apply(data, data, 0)

	utils.AssertEqual(t, data.Count.Value, float64(4))
}

func TestCountOp_Merge(t *testing.T) {
	data := core.NewDataTable()
	mergingData := make([]core.DataTable, 0)
	for i := 0; i < 5; i++ {
		mergeData := core.NewDataTable()
		mergeData.Count.Value = float64(i)
		mergingData = append(mergingData, *mergeData)
	}

	op := NewCountOp()
	op.Merge(data, mergingData)

	utils.AssertEqual(t, data.Count.Value, float64(10))
}

func TestCountOp_Query(t *testing.T) {
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

	op := NewCountOp()
	queryParams := &QueryParams{
		ConfidenceLevel: 0.5,
		SDMultiplier:    1,
	}

	agg := op.Query(summaryWindows, landmarkWindows, 1, 21, queryParams)
	utils.AssertEqual(t, agg.value.Count.Value, 6.9)
	utils.AssertClose(t, agg.error, 9.442857e-1, 1e-7)
}
