package core

type CountData struct {
	Value float64
}

type DataTable struct {
	Count *CountData
}

func NewDataTable() *DataTable {
	return &DataTable{
		Count: &CountData{},
	}
}
