package operator

import (
	"summarydb/core"
)

type QueryParams struct {
	ConfidenceLevel float64
	SDMultiplier    float64
}

type AggResult struct {
	value *core.DataTable
	error float64
}

type Op interface {
	Apply(*core.DataTable, *core.DataTable)
	Merge(*core.DataTable, []core.DataTable)
	EmptyQuery() *AggResult
	Query([]core.SummaryWindow, []core.LandmarkWindow, int64, int64, *QueryParams) *AggResult
}
