package operator

import (
	"summarydb/core"
	"summarydb/protos"
)

type AggResult struct {
	value int64
	error float32
}

type Op interface {
	Apply(int64, int64) int64
	Merge([]int64) int64
	EmptyQuery() *AggResult
	Query([]core.SummaryWindow, int64, int64) *AggResult
	Serialize(int64) protos.ProtoOperator
	Deserialize(protos.ProtoOperator) int64
}
