package core

import (
	"summarydb/protos"
	"summarydb/stats"
)

type SumOp struct {
	OpType protos.OpType
}

func NewSumOp() *SumOp {
	return &SumOp{
		OpType: protos.OpType_sum,
	}
}

func (op *SumOp) Apply(retData, aggData *DataTable, insertValue float64, _ int64) {
	retData.Sum.Value = aggData.Sum.Value + insertValue
}

func (op *SumOp) Merge(retData *DataTable, values []DataTable) {
	for _, value := range values {
		retData.Sum.Value += value.Sum.Value
	}
}

func (op *SumOp) EmptyQuery() *AggResult {
	return &AggResult{
		value: NewDataTable(),
		error: 0,
	}
}

// TODO: Add stream statistics, and get SDMultiplier
func (op *SumOp) Query(windows []*SummaryWindow,
	landmarkWindows []*LandmarkWindow,
	t0 int64, t1 int64,
	params *QueryParams) *AggResult {

	bounds, meanvar := GetSumStats(t0, t1,
		windows,
		landmarkWindows,
		func(table *DataTable) float64 {
			return table.Sum.Value
		})

	ci := stats.ConvertStatsBoundsToCI(
		bounds,
		meanvar,
		params.SDMultiplier,
		params.ConfidenceLevel)

	aggData := NewDataTable()
	aggData.Sum.Value = ci.Mean

	return &AggResult{
		value: aggData,
		error: ci.UpperCI - ci.LowerCI,
	}
}
