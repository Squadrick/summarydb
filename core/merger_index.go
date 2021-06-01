package core

import (
	"math"
	"summarydb/tree"
)

const InvalidInt64 int64 = math.MinInt64

type MergerIndexItem struct {
	cEnd     int64
	heapItem *tree.HeapItem
}

// In-memory index mapping swid -> (cEnd, heapItem)
// Where,
//		cEnd is the end timestamp
// 		heapItem is a pointer to an element in the main merge heap (mergeCounts)
// To support predecessor/successor lookups, we use a RB tree instead of hashmap
type MergerIndex struct {
	indexMap *tree.RbTree
}

func NewMergerIndex() *MergerIndex {
	return &MergerIndex{indexMap: tree.NewRbTree()}
}

func (index *MergerIndex) PopulateFromHeap(heap *tree.MinHeap) {
	for _, entry := range *heap {
		item, ok := index.indexMap.Get(entry.Value)
		if !ok {
			continue
		}
		indexItem := item.(*MergerIndexItem)
		indexItem.heapItem = entry
	}
}

func (index *MergerIndex) Put(swid int64, cEnd int64) {
	if swid == InvalidInt64 {
		return
	}
	item := &MergerIndexItem{
		cEnd:     cEnd,
		heapItem: nil,
	}
	index.indexMap.Insert(swid, item)
}

func (index *MergerIndex) Remove(swid int64) *MergerIndexItem {
	if swid == InvalidInt64 {
		return nil
	}
	item, ok := index.indexMap.Get(swid)
	if !ok {
		return nil
	}
	indexItem := item.(*MergerIndexItem)
	index.indexMap.Delete(swid)
	return indexItem
}

func (index *MergerIndex) Contains(swid int64) bool {
	if swid == InvalidInt64 {
		return false
	}
	return index.indexMap.Exists(swid)
}

func (index *MergerIndex) GetCStart(swid int64) int64 {
	if !index.Contains(swid) || swid == InvalidInt64 {
		return InvalidInt64
	}
	_, prevItem := index.indexMap.Lower(swid)
	if prevItem == nil {
		return 0
	}
	indexItem := prevItem.(*MergerIndexItem)
	return indexItem.cEnd + 1
}

func (index *MergerIndex) GetCEnd(swid int64) int64 {
	if swid == InvalidInt64 {
		return InvalidInt64
	}
	item, ok := index.indexMap.Get(swid)
	if !ok {
		return InvalidInt64
	}
	indexItem := item.(*MergerIndexItem)
	return indexItem.cEnd
}

func (index *MergerIndex) GetPred(swid int64) int64 {
	if !index.Contains(swid) || swid == InvalidInt64 {
		return InvalidInt64
	}
	prevKey, _ := index.indexMap.Lower(swid)
	if prevKey == tree.InvalidRbKey {
		return InvalidInt64
	}
	return prevKey
}

func (index *MergerIndex) GetSucc(swid int64) int64 {
	if !index.Contains(swid) || swid == InvalidInt64 {
		return InvalidInt64
	}
	//key := tree.Int64Key(swid)
	succKey, _ := index.indexMap.Higher(swid)
	if succKey == tree.InvalidRbKey {
		return InvalidInt64
	}
	return succKey
}

func (index *MergerIndex) GetLastSWID() int64 {
	if index.indexMap.IsEmpty() {
		return InvalidInt64
	}

	maxKey, _ := index.indexMap.Max()
	return maxKey
}

func (index *MergerIndex) UnsetHeapItem(swid int64) *tree.HeapItem {
	if swid == InvalidInt64 {
		return nil
	}
	item, ok := index.indexMap.Get(swid)
	if !ok {
		return nil
	}
	indexItem := item.(*MergerIndexItem)
	heapPtr := indexItem.heapItem
	indexItem.heapItem = nil
	return heapPtr
}

func (index *MergerIndex) SetHeapItem(swid int64, heapItem *tree.HeapItem) bool {
	if swid == InvalidInt64 {
		return false
	}
	item, ok := index.indexMap.Get(swid)
	if !ok {
		return ok
	}
	indexItem := item.(*MergerIndexItem)
	indexItem.heapItem = heapItem
	return true
}
