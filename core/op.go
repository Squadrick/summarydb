package core

type QueryParams struct {
	ConfidenceLevel float64
	SDMultiplier    float64
}

type AggResult struct {
	value *DataTable
	error float64
}

type Op interface {
	Apply(*DataTable, *DataTable, float64, int64)
	Merge(*DataTable, []DataTable)
	EmptyQuery() *AggResult
	Query([]SummaryWindow, []LandmarkWindow, int64, int64, *QueryParams) *AggResult
}
