package core

import "summarydb/protos"
import "reflect"

// TODO: Remove op names entirely. Only work with ops and op protos.

var OpTypeOpStringMap = map[protos.OpType]string{
	protos.OpType_sum:   "sum",
	protos.OpType_count: "count",
	protos.OpType_max:   "max",
}

var OpNameOpTypeMap = map[string]Op{
	"sum":   NewSumOp(),
	"count": NewCountOp(),
	"max":   NewMaxOp(),
}

type OpSet struct {
	ops map[string]Op
}

func NewOpSet(operatorNames []string) *OpSet {
	ops := make(map[string]Op)
	for _, operatorName := range operatorNames {
		ops[operatorName] = OpNameOpTypeMap[operatorName]
	}
	return &OpSet{ops: ops}
}

func OpProtosToOpNames(opsProto protos.OpType_List) []string {
	opNames := make([]string, opsProto.Len())
	for i := 0; i < opsProto.Len(); i += 1 {
		opNames[i] = OpTypeOpStringMap[opsProto.At(i)]
	}
	return opNames
}

func (set *OpSet) GetOp(operatorName string) Op {
	return set.ops[operatorName]
}

func (set *OpSet) Insert(data *DataTable, value float64, ts int64) {
	for _, op := range set.ops {
		op.Apply(data, data, value, ts)
	}
}

func (set *OpSet) Merge(data []DataTable) *DataTable {
	// TODO: Since each Op is associative, we can parallelize or
	// divide-and-conquer rather than doing it linearly. Rather than using a
	// single approach, the algorithm selection should be based on the size
	// of data. For smaller sizes use linear, for medium use parallel.
	mergedData := NewDataTable()
	for _, op := range set.ops {
		op.Merge(mergedData, data)
	}
	return mergedData
}

func (set *OpSet) Equals(other *OpSet) bool {
	return reflect.DeepEqual(set, other)
}
