package monoid

import (
	"math"
	"summarydb/protos"
)

type Op interface {
	Apply(*DataType, *DataType) *DataType
}

type MaxOp struct {
	OpType protos.OpType
}

func NewMaxOp() *MaxOp {
	return &MaxOp{OpType: protos.OpType_max}
}

func (maxOp *MaxOp) Apply(a, b *DataType) *DataType {
	c := NewDataTable()
	c.Max = math.Max(a.Max, b.Max)
	return c
}
