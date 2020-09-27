package operator

import (
	"summarydb/core"
	"summarydb/utils"
	"testing"
)

func TestMaxOp_Apply(t *testing.T) {
	data := core.NewDataTable()
	data.Max.Value = 3

	insert := core.NewDataTable()
	insert.Max.Value = 5

	op := NewMaxOp()
	op.Apply(data, data, insert, 0)

	utils.AssertEqual(t, data.Max.Value, float64(5))
}

func TestMaxOp_Merge(t *testing.T) {
	data := core.NewDataTable()
	mergingData := make([]core.DataTable, 0)
	for i := 0; i < 5; i++ {
		mergeData := core.NewDataTable()
		mergeData.Max.Value = float64(i)
		mergingData = append(mergingData, *mergeData)
	}

	op := NewMaxOp()
	op.Merge(data, mergingData)

	utils.AssertEqual(t, data.Max.Value, float64(4))
}
