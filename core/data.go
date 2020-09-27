package core

import "math"

type Scalar struct {
	Value float64
}

type DataTable struct {
	Count *Scalar
	Sum   *Scalar
	Max   *Scalar
}

func NewDataTable() *DataTable {
	return &DataTable{
		Count: &Scalar{},
		Sum:   &Scalar{},
		Max: &Scalar{
			Value: -math.MaxFloat64,
		},
	}
}
