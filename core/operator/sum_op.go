package operator

import (
	"summarydb/core"
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

func (op *SumOp) Apply(retData, aggData, insertData *core.DataTable, ts int64) {
	retData.Sum.Value = aggData.Sum.Value + insertData.Sum.Value
}

func (op *SumOp) Merge(retData *core.DataTable, values []core.DataTable) {
	for _, value := range values {
		retData.Sum.Value += value.Count.Value
	}
}

func (op *SumOp) EmptyQuery() *AggResult {
	return &AggResult{
		value: core.NewDataTable(),
		error: 0,
	}
}

// TODO: Add stream statistics, and get SDMultiplier
func (op *SumOp) Query(windows []core.SummaryWindow,
	landmarkWindows []core.LandmarkWindow,
	t0 int64, t1 int64,
	params *QueryParams) *AggResult {

	bounds, meanvar := stats.GetSumStats(t0, t1,
		windows,
		landmarkWindows,
		func(table *core.DataTable) float64 {
			return table.Max.Value
		})

	ci := stats.ConvertStatsBoundsToCI(
		bounds,
		meanvar,
		params.SDMultiplier,
		params.ConfidenceLevel)

	aggData := core.NewDataTable()
	aggData.Count.Value = ci.Mean

	return &AggResult{
		value: aggData,
		error: ci.UpperCI - ci.LowerCI,
	}
}
