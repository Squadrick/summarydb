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
		Count: &Scalar{Value: 0.0},
		Sum:   &Scalar{Value: 0.0},
		Max:   &Scalar{Value: -math.MaxFloat64},
	}
}
