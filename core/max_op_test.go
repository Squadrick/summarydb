package core

import (
	"summarydb/utils"
	"testing"
)

func TestMaxOp_Apply(t *testing.T) {
	data := NewDataTable()
	data.Max.Value = 3

	op := NewMaxOp()
	op.Apply(data, data, 5.0, 0)

	utils.AssertEqual(t, data.Max.Value, float64(5))
}

func TestMaxOp_Merge(t *testing.T) {
	data := NewDataTable()
	mergingData := make([]DataTable, 0)
	for i := 0; i < 5; i++ {
		mergeData := NewDataTable()
		mergeData.Max.Value = float64(i)
		mergingData = append(mergingData, *mergeData)
	}

	op := NewMaxOp()
	op.Merge(data, mergingData)

	utils.AssertEqual(t, data.Max.Value, float64(4))
}
