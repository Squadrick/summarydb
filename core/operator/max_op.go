package operator

import (
	"math"
	"summarydb/core"
	"summarydb/protos"
)

type MaxOp struct {
	OpType protos.OpType
}

func NewMaxOp() *MaxOp {
	return &MaxOp{
		OpType: protos.OpType_max,
	}
}

func (op *MaxOp) Apply(retData, aggData, insertData *core.DataTable, ts int64) {
	retData.Max.Value = math.Max(aggData.Max.Value, insertData.Max.Value)
}

func (op *MaxOp) Merge(retData *core.DataTable, values []core.DataTable) {
	for _, value := range values {
		retData.Max.Value = math.Max(retData.Max.Value, value.Max.Value)
	}
}

func (op *MaxOp) EmptyQuery() *AggResult {
	return &AggResult{
		value: core.NewDataTable(),
		error: 1.0,
	}
}

func (op *MaxOp) Query(windows []core.SummaryWindow,
	landmarkWindows []core.LandmarkWindow,
	t0 int64, t1 int64,
	params *QueryParams) *AggResult {

	aggResult := op.EmptyQuery()

	datas := make([]core.DataTable, len(windows))
	for i, window := range windows {
		datas[i] = *window.Data
	}
	op.Merge(aggResult.value, datas)

	for _, window := range landmarkWindows {
		for _, landmark := range window.Landmarks {
			if landmark.Timestamp >= t0 && landmark.Timestamp <= t1 {
				aggResult.value.Max.Value = math.Max(aggResult.value.Max.Value,
					landmark.Data.Max.Value)
				aggResult.error = 0.0
			}
		}
	}

	return aggResult
}
