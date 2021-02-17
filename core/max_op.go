package core

import (
	"math"
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

func (op *MaxOp) Apply(retData, aggData *DataTable, insertValue float64, _ int64) {
	retData.Max.Value = math.Max(aggData.Max.Value, insertValue)
}

func (op *MaxOp) Merge(retData *DataTable, values []DataTable) {
	for _, value := range values {
		retData.Max.Value = math.Max(retData.Max.Value, value.Max.Value)
	}
}

func (op *MaxOp) EmptyQuery() *AggResult {
	return &AggResult{
		value: NewDataTable(),
		error: 1.0,
	}
}

func (op *MaxOp) Query(windows []*SummaryWindow,
	landmarkWindows []*LandmarkWindow,
	t0 int64, t1 int64,
	_ *QueryParams) *AggResult {

	aggResult := op.EmptyQuery()

	datas := make([]DataTable, len(windows))
	for i, window := range windows {
		datas[i] = *window.Data
	}
	op.Merge(aggResult.value, datas)

	for _, window := range landmarkWindows {
		for _, landmark := range window.Landmarks {
			if landmark.Timestamp >= t0 && landmark.Timestamp <= t1 {
				aggResult.value.Max.Value = math.Max(aggResult.value.Max.Value,
					landmark.Value)
				aggResult.error = 0.0
			}
		}
	}

	return aggResult
}
