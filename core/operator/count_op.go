package operator

import (
	"summarydb/core"
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

func (op *CountOp) Apply(retData, data *core.DataTable, ts int64) {
	retData.Count.Value = data.Count.Value + 1
}

func (op *CountOp) Merge(retData *core.DataTable, values []core.DataTable) {
	for _, value := range values {
		retData.Count.Value += value.Count.Value
	}
}

func (op *CountOp) EmptyQuery() *AggResult {
	return &AggResult{
		value: core.NewDataTable(),
		error: 0,
	}
}

func (op *CountOp) Query(windows []core.SummaryWindow,
	landmarkWindows []core.LandmarkWindow,
	t0 int64, t1 int64,
	params *QueryParams) *AggResult {

	bounds, meanvar := stats.GetSumStats(t0, t1,
		windows,
		landmarkWindows,
		func(table *core.DataTable) float64 {
			return table.Count.Value
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
