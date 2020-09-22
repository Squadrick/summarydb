package operator

import (
	"summarydb/core"
	"summarydb/protos"
	"summarydb/stats"
)

type CountOp struct {
	OpType protos.OpType
	count  int64
}

func NewCountOp() *CountOp {
	return &CountOp{
		OpType: protos.OpType_count,
		count:  0,
	}
}

func (op *CountOp) Apply(value int64, ts int64) int64 {
	return value + 1
}

func (op *CountOp) Merge(values []int64) int64 {
	sum := int64(0)
	for _, value := range values {
		sum += value
	}
	return sum
}

func (op *CountOp) EmptyQuery() *AggResult {
	return &AggResult{
		value: 0,
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

	return &AggResult{
		value: int64(ci.Mean),
		error: (ci.LowerCI + ci.UpperCI) / 2.0,
	}
}
