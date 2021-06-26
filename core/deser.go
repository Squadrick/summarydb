package core

import (
	"capnproto.org/go/capnp/v3"
	"summarydb/protos"
	"summarydb/tree"
)

// NOTE: Tests are in backing_store_test.go

func SummaryWindowToBytes(window *SummaryWindow) ([]byte, error) {
	msg, seg, err := capnp.NewMessage(capnp.SingleSegment(nil))
	if err != nil {
		return nil, err
	}

	summaryWindowProto, err := protos.NewRootProtoSummaryWindow(seg)
	if err != nil {
		return nil, err
	}

	summaryWindowProto.SetTs(window.TimeStart)
	summaryWindowProto.SetTe(window.TimeEnd)
	summaryWindowProto.SetCs(window.CountStart)
	summaryWindowProto.SetCe(window.CountEnd)

	dataTableProto, err := summaryWindowProto.NewOpData()
	if err != nil {
		return nil, err
	}

	dataTableProto.SetCount(window.Data.Count.Value)
	dataTableProto.SetMax(window.Data.Max.Value)
	dataTableProto.SetSum(window.Data.Sum.Value)

	buf, err := msg.Marshal()
	if err != nil {
		return nil, err
	}

	return buf, nil
}

func BytesToSummaryWindow(buf []byte) (*SummaryWindow, error) {
	msg, err := capnp.Unmarshal(buf)
	if err != nil {
		return nil, err
	}

	summaryWindowProto, err := protos.ReadRootProtoSummaryWindow(msg)
	if err != nil {
		return nil, err
	}

	summaryWindow := NewSummaryWindow(
		summaryWindowProto.Ts(),
		summaryWindowProto.Te(),
		summaryWindowProto.Cs(),
		summaryWindowProto.Ce())
	dataTableProto, err := summaryWindowProto.OpData()
	if err != nil {
		return nil, err
	}

	summaryWindow.Data.Sum.Value = dataTableProto.Sum()
	summaryWindow.Data.Count.Value = dataTableProto.Count()
	summaryWindow.Data.Max.Value = dataTableProto.Max()
	return summaryWindow, nil
}

func LandmarkWindowToBytes(window *LandmarkWindow) ([]byte, error) {
	msg, seg, err := capnp.NewMessage(capnp.SingleSegment(nil))
	if err != nil {
		return nil, err
	}

	landmarkWindowProto, err := protos.NewRootProtoLandmarkWindow(seg)
	if err != nil {
		return nil, err
	}

	landmarkWindowProto.SetTs(window.TimeStart)
	landmarkWindowProto.SetTe(window.TimeEnd)

	timestampsProto, err :=
		landmarkWindowProto.NewTimestamps(int32(len(window.Landmarks)))
	if err != nil {
		return nil, err
	}
	valuesProto, err :=
		landmarkWindowProto.NewValues(int32(len(window.Landmarks)))
	if err != nil {
		return nil, err
	}

	for i, landmark := range window.Landmarks {
		timestampsProto.Set(i, landmark.Timestamp)
		valuesProto.Set(i, landmark.Value)
	}

	buf, err := msg.Marshal()
	if err != nil {
		return nil, err
	}

	return buf, nil
}

func BytesToLandmarkWindow(buf []byte) (*LandmarkWindow, error) {
	msg, err := capnp.Unmarshal(buf)
	if err != nil {
		return nil, err
	}
	landmarkWindowProto, err := protos.ReadRootProtoLandmarkWindow(msg)
	if err != nil {
		return nil, err
	}

	landmarkWindow := NewLandmarkWindow(landmarkWindowProto.Ts())
	timestampsProto, err := landmarkWindowProto.Timestamps()
	if err != nil {
		return nil, err
	}
	valuesProto, err := landmarkWindowProto.Values()
	if err != nil {
		return nil, err
	}

	for i := 0; i < valuesProto.Len(); i++ {
		landmarkWindow.Insert(timestampsProto.At(i), valuesProto.At(i))
	}

	landmarkWindow.Close(landmarkWindowProto.Te())
	return landmarkWindow, nil
}

func HeapToBytes(heap *tree.MinHeap) ([]byte, error) {
	msg, seg, err := capnp.NewMessage(capnp.SingleSegment(nil))
	if err != nil {
		return nil, err
	}

	heapProto, err := protos.NewRootHeap(seg)
	if err != nil {
		return nil, err
	}

	heapItemsProto, err := heapProto.NewItems(int32(len(*heap)))
	if err != nil {
		return nil, err
	}

	for i, it := range *heap {
		heapItemProto, err := protos.NewHeapItem(seg)
		if err != nil {
			return nil, err
		}
		heapItemProto.SetValue(it.Value)
		heapItemProto.SetIndex(int32(it.Index))
		heapItemProto.SetPriority(int32(it.Priority))
		err = heapItemsProto.Set(i, heapItemProto)
		if err != nil {
			return nil, err
		}
	}

	buf, err := msg.Marshal()
	if err != nil {
		return nil, err
	}
	return buf, nil
}

func BytesToHeap(buf []byte) (*tree.MinHeap, error) {
	msg, err := capnp.Unmarshal(buf)
	if err != nil {
		return nil, err
	}
	heapProto, err := protos.ReadRootHeap(msg)
	if err != nil {
		return nil, err
	}
	heapItemsProto, err := heapProto.Items()
	if err != nil {
		return nil, err
	}

	heap := tree.NewMinHeap(heapItemsProto.Len())
	for i := 0; i < heapItemsProto.Len(); i++ {
		heapItemProto := heapItemsProto.At(i)
		heapItem := &tree.HeapItem{
			Value:    heapItemProto.Value(),
			Priority: int(heapItemProto.Priority()),
			Index:    int(heapItemProto.Index()),
		}
		*heap = append(*heap, heapItem)
	}
	return heap, nil
}

func MergerIndexToBytes(index *MergerIndex) ([]byte, error) {
	msg, seg, err := capnp.NewMessage(capnp.SingleSegment(nil))

	indexProto, err := protos.NewRootMergerIndex(seg)
	if err != nil {
		return nil, err
	}
	idx := 0
	indexItemsProto, err := indexProto.NewItems(int32(index.indexMap.Count()))
	if err != nil {
		return nil, err
	}

	index.indexMap.Map(func(key tree.RbKey, val interface{}) bool {
		indexItemProto, err := protos.NewMergerIndexItem(seg)
		if err != nil {
			return true
		}
		indexItemProto.SetSwid(key)
		indexItemProto.SetCEnd(val.(*MergerIndexItem).cEnd)
		err = indexItemsProto.Set(idx, indexItemProto)
		if err != nil {
			return true
		}
		idx += 1
		return false
	})

	buf, err := msg.Marshal()
	if err != nil {
		return nil, err
	}
	return buf, nil
}

func BytesToMergerIndex(buf []byte) (*MergerIndex, error) {
	msg, err := capnp.Unmarshal(buf)
	if err != nil {
		return nil, err
	}
	indexProto, err := protos.ReadRootMergerIndex(msg)
	if err != nil {
		return nil, err
	}
	indexItemsProto, err := indexProto.Items()
	if err != nil {
		return nil, err
	}

	mergerIndex := NewMergerIndex()
	for i := 0; i < indexItemsProto.Len(); i++ {
		indexItemProto := indexItemsProto.At(i)
		mergerIndex.Put(indexItemProto.Swid(), indexItemProto.CEnd())
	}
	return mergerIndex, nil
}
