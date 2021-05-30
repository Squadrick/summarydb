package monoid

import "math"

type DataType struct {
	Count float64
	Sum   float64
	Max   float64
}

func NewDataTable() *DataType {
	return &DataType{
		Count: 0,
		Sum:   0,
		Max:   -math.MaxFloat64,
	}
}
