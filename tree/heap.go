package tree

import "container/heap"

type HeapItem struct {
	Value    int64
	Priority int
	Index    int
}

type MinHeap []*HeapItem

func (mh MinHeap) Len() int {
	return len(mh)
}

func (mh MinHeap) Less(i, j int) bool {
	if mh[i].Priority == mh[j].Priority {
		return mh[i].Value < mh[j].Value
	} else {
		return mh[i].Priority < mh[j].Priority
	}
}

func (mh MinHeap) Swap(i, j int) {
	mh[i], mh[j] = mh[j], mh[i]
	mh[i].Index = i
	mh[j].Index = j
}

func (mh *MinHeap) Push(x interface{}) {
	n := len(*mh)
	item := x.(*HeapItem)
	item.Index = n
	*mh = append(*mh, item)
}

func (mh *MinHeap) Pop() interface{} {
	old := *mh
	n := len(old)
	item := old[n-1]
	old[n-1] = nil // avoid memory leak
	item.Index = -1
	*mh = old[0 : n-1]
	return item
}

func (mh *MinHeap) Top() interface{} {
	arr := *mh
	item := arr[0]
	return item
}

func (mh *MinHeap) Update(item *HeapItem, value int64, priority int) {
	item.Value = value
	item.Priority = priority
	heap.Fix(mh, item.Index)
}

func NewMinHeap(initSize int) *MinHeap {
	mh := make(MinHeap, 0, initSize)
	heap.Init(&mh)
	return &mh
}
