package operator

import (
	"summarydb/core"
	"summarydb/protos"
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

func (op *CountOp) Query(windows []core.SummaryWindow, t0 int64, t1 int64) *AggResult {
	return op.EmptyQuery()
}
