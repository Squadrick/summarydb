package operator

import (
	"summarydb/core"
	"summarydb/protos"
)

type QueryParams struct {
	ConfidenceLevel float64
	SDMultiplier    float64
}

type AggResult struct {
	value int64
	error float64
}

type Op interface {
	Apply(int64, int64) int64
	Merge([]int64) int64
	EmptyQuery() *AggResult
	Query([]core.SummaryWindow, []core.LandmarkWindow, int64, int64, *QueryParams) *AggResult
	Serialize(int64) protos.ProtoOperator
	Deserialize(protos.ProtoOperator) int64
}
