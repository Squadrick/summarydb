package core

func GetOpFromName(opName string) Op {
	if opName == "sum" {
		return NewSumOp()
	} else if opName == "count" {
		return NewCountOp()
	} else if opName == "max" {
		return NewMaxOp()
	} else {
		return nil
	}
}

type OpSet struct {
	ops map[string]Op
}

func NewOpSet(operatorNames []string) *OpSet {
	ops := make(map[string]Op)
	for _, operatorName := range operatorNames {
		op := GetOpFromName(operatorName)
		if op == nil {
			// invalid op
			continue
		}
		ops[operatorName] = GetOpFromName(operatorName)
	}
	return &OpSet{ops: ops}
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
