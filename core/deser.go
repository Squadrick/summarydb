package core

import (
	"capnproto.org/go/capnp/v3"
	"summarydb/protos"
	"summarydb/tree"
)

// NOTE: Tests are in backing_store_test.go

func SummaryWindowToBytes(window *SummaryWindow) []byte {
	msg, seg, err := capnp.NewMessage(capnp.SingleSegment(nil))

	summaryWindowProto, err := protos.NewRootProtoSummaryWindow(seg)
	if err != nil {
		return nil
	}

	summaryWindowProto.SetTs(window.TimeStart)
	summaryWindowProto.SetTe(window.TimeEnd)
	summaryWindowProto.SetCs(window.CountStart)
	summaryWindowProto.SetCe(window.CountEnd)

	dataTableProto, err := summaryWindowProto.NewOpData()
	if err != nil {
		return nil
	}

	dataTableProto.SetCount(window.Data.Count.Value)
	dataTableProto.SetMax(window.Data.Max.Value)
	dataTableProto.SetSum(window.Data.Sum.Value)

	buf, err := msg.Marshal()
	if err != nil {
		return nil
	}

	return buf
}

func BytesToSummaryWindow(buf []byte) *SummaryWindow {
	msg, err := capnp.Unmarshal(buf)
	if err != nil {
		return nil
	}

	summaryWindowProto, err := protos.ReadRootProtoSummaryWindow(msg)
	if err != nil {
		return nil
	}

	summaryWindow := NewSummaryWindow(
		summaryWindowProto.Ts(),
		summaryWindowProto.Te(),
		summaryWindowProto.Cs(),
		summaryWindowProto.Ce())
	dataTableProto, err := summaryWindowProto.OpData()
	if err != nil {
		return summaryWindow
	}

	summaryWindow.Data.Sum.Value = dataTableProto.Sum()
	summaryWindow.Data.Count.Value = dataTableProto.Count()
	summaryWindow.Data.Max.Value = dataTableProto.Max()
	return summaryWindow
}

func LandmarkWindowToBytes(window *LandmarkWindow) []byte {
	msg, seg, err := capnp.NewMessage(capnp.SingleSegment(nil))

	landmarkWindowProto, err := protos.NewRootProtoLandmarkWindow(seg)
	if err != nil {
		return nil
	}

	landmarkWindowProto.SetTs(window.TimeStart)
	landmarkWindowProto.SetTe(window.TimeEnd)

	timestampsProto, _ := landmarkWindowProto.NewTimestamps(int32(len(window.Landmarks)))
	valuesProto, _ := landmarkWindowProto.NewValues(int32(len(window.Landmarks)))

	for i, landmark := range window.Landmarks {
		timestampsProto.Set(i, landmark.Timestamp)
		valuesProto.Set(i, landmark.Value)
	}

	buf, err := msg.Marshal()
	if err != nil {
		return nil
	}

	return buf
}

func BytesToLandmarkWindow(buf []byte) *LandmarkWindow {
	msg, err := capnp.Unmarshal(buf)
	if err != nil {
		return nil
	}
	landmarkWindowProto, err := protos.ReadRootProtoLandmarkWindow(msg)
	if err != nil {
		return nil
	}

	landmarkWindow := NewLandmarkWindow(landmarkWindowProto.Ts())
	timestampsProto, _ := landmarkWindowProto.Timestamps()
	valuesProto, _ := landmarkWindowProto.Values()

	for i := 0; i < valuesProto.Len(); i++ {
		landmarkWindow.Insert(timestampsProto.At(i), valuesProto.At(i))
	}

	landmarkWindow.Close(landmarkWindowProto.Te())
	return landmarkWindow
}

func HeapToBytes(heap *tree.MinHeap) []byte {
	msg, seg, err := capnp.NewMessage(capnp.SingleSegment(nil))
	if err != nil {
		panic(err)
	}

	heapProto, err := protos.NewRootHeap(seg)
	if err != nil {
		panic(err)
	}

	heapItemsProto, err := heapProto.NewItems(int32(len(*heap)))
	if err != nil {
		panic(err)
	}

	for i, it := range *heap {
		heapItemProto, err := protos.NewHeapItem(seg)
		if err != nil {
			panic(err)
		}
		heapItemProto.SetValue(it.Value)
		heapItemProto.SetIndex(int32(it.Index))
		heapItemProto.SetPriority(int32(it.Priority))
		err = heapItemsProto.Set(i, heapItemProto)
		if err != nil {
			panic(err)
		}
	}

	buf, err := msg.Marshal()
	if err != nil {
		panic(err)
	}
	return buf
}

func BytesToHeap(buf []byte) *tree.MinHeap {
	msg, err := capnp.Unmarshal(buf)
	if err != nil {
		panic(err)
	}
	heapProto, err := protos.ReadRootHeap(msg)
	if err != nil {
		panic(err)
	}
	heapItemsProto, err := heapProto.Items()
	if err != nil {
		panic(err)
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
	return heap
}

func MergerIndexToBytes(index *MergerIndex) []byte {
	msg, seg, err := capnp.NewMessage(capnp.SingleSegment(nil))

	indexProto, err := protos.NewRootMergerIndex(seg)
	if err != nil {
		panic(err)
	}
	idx := 0
	indexItemsProto, err := indexProto.NewItems(int32(index.indexMap.Count()))
	if err != nil {
		panic(err)
	}

	index.indexMap.Map(func(key tree.RbKey, val interface{}) bool {
		indexItemProto, err := protos.NewMergerIndexItem(seg)
		if err != nil {
			panic(err)
		}
		indexItemProto.SetSwid(key)
		indexItemProto.SetCEnd(val.(*MergerIndexItem).cEnd)
		err = indexItemsProto.Set(idx, indexItemProto)
		if err != nil {
			panic(err)
		}
		idx += 1
		return false
	})

	buf, err := msg.Marshal()
	if err != nil {
		panic(err)
	}
	return buf
}

func BytesToMergerIndex(buf []byte) *MergerIndex {
	msg, err := capnp.Unmarshal(buf)
	if err != nil {
		panic(err)
	}
	indexProto, err := protos.ReadRootMergerIndex(msg)
	if err != nil {
		panic(err)
	}
	indexItemsProto, err := indexProto.Items()
	if err != nil {
		panic(err)
	}

	mergerIndex := NewMergerIndex()
	for i := 0; i < indexItemsProto.Len(); i++ {
		indexItemProto := indexItemsProto.At(i)
		mergerIndex.Put(indexItemProto.Swid(), indexItemProto.CEnd())
	}
	return mergerIndex
}
