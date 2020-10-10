package core

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestCountOp_Apply(t *testing.T) {
	data := NewDataTable()
	data.Count.Value = 3

	op := NewCountOp()
	op.Apply(data, data, 0.0, 0)

	assert.Equal(t, data.Count.Value, float64(4))
}

func TestCountOp_Merge(t *testing.T) {
	data := NewDataTable()
	mergingData := make([]DataTable, 0)
	for i := 0; i < 5; i++ {
		mergeData := NewDataTable()
		mergeData.Count.Value = float64(i)
		mergingData = append(mergingData, *mergeData)
	}

	op := NewCountOp()
	op.Merge(data, mergingData)

	assert.Equal(t, data.Count.Value, float64(10))
}

func TestCountOp_Query(t *testing.T) {
	summaryWindows := make([]SummaryWindow, 0)
	landmarkWindows := make([]LandmarkWindow, 0)

	for i := int64(0); i < 5; i++ {
		if i == 2 {
			continue
		}
		summaryWindow := NewSummaryWindow(i*5, (i+1)*5-1, i, i+1)
		summaryWindow.Data.Count.Value = 1
		summaryWindows = append(summaryWindows, *summaryWindow)
	}

	landmarkWindow1 := NewLandmarkWindow(2)
	landmarkWindow1.Insert(3, 1.0)
	landmarkWindow1.Close(5)

	landmarkWindow2 := NewLandmarkWindow(5)
	landmarkWindow2.Insert(6, 1.0)
	landmarkWindow2.Insert(7, 1.0)
	landmarkWindow2.Insert(8, 1.0)
	landmarkWindow2.Close(9)

	landmarkWindows = append(landmarkWindows, *landmarkWindow1, *landmarkWindow2)

	op := NewCountOp()
	queryParams := &QueryParams{
		ConfidenceLevel: 0.5,
		SDMultiplier:    1,
	}

	agg := op.Query(summaryWindows, landmarkWindows, 1, 21, queryParams)
	assert.InEpsilonf(t, agg.value.Count.Value, 6.9, 1e-6, "Count value")
	assert.InEpsilon(t, agg.error, 9.442857e-1, 1e-7, "Error value")
}
