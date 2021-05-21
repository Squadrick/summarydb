package core

import (
	"summarydb/protos"
	"summarydb/stats"
)

type CountOp struct {
	OpType protos.OpType
}

func NewCountOp() *CountOp {
	return &CountOp{
		OpType: protos.OpType_count,
	}
}

func (op *CountOp) GetOpType() protos.OpType {
	return op.OpType
}

func (op *CountOp) Apply(retData, aggData *DataTable, _ float64, _ int64) {
	retData.Count.Value = aggData.Count.Value + 1
}

func (op *CountOp) Merge(retData *DataTable, values []DataTable) {
	for _, value := range values {
		retData.Count.Value += value.Count.Value
	}
}

func (op *CountOp) EmptyQuery() *AggResult {
	return &AggResult{
		value: NewDataTable(),
		error: 0,
	}
}

func (op *CountOp) Query(windows []*SummaryWindow,
	landmarkWindows []*LandmarkWindow,
	t0 int64, t1 int64,
	params *QueryParams) *AggResult {

	bounds, meanvar := GetSumStats(t0, t1,
		windows,
		landmarkWindows,
		func(table *DataTable) float64 {
			return table.Count.Value
		})

	ci := stats.ConvertStatsBoundsToCI(
		bounds,
		meanvar,
		params.SDMultiplier,
		params.ConfidenceLevel)

	aggData := NewDataTable()
	aggData.Count.Value = ci.Mean

	return &AggResult{
		value: aggData,
		error: ci.UpperCI - ci.LowerCI,
	}
}
