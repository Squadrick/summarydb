package tree

import "container/heap"

type Item struct {
	value    int64
	priority int
	index    int
}

type MinHeap []*Item

func (mh MinHeap) Len() int {
	return len(mh)
}

func (mh MinHeap) Less(i, j int) bool {
	return mh[i].priority > mh[j].priority
}

func (mh MinHeap) Swap(i, j int) {
	mh[i], mh[j] = mh[j], mh[i]
	mh[i].index = i
	mh[j].index = j
}

func (mh *MinHeap) Push(x interface{}) {
	n := len(*mh)
	item := x.(*Item)
	item.index = n
	*mh = append(*mh, item)
}

func (mh *MinHeap) Pop() interface{} {
	old := *mh
	n := len(old)
	item := old[n-1]
	old[n-1] = nil // avoid memory leak
	item.index = -1
	*mh = old[0 : n-1]
	return item
}

func (mh *MinHeap) Top() interface{} {
	arr := *mh
	n := len(arr)
	item := arr[n-1]
	return item
}

func (mh *MinHeap) Update(item *Item, value int64, priority int) {
	item.value = value
	item.priority = priority
	heap.Fix(mh, item.index)
}

func (mh *MinHeap) Delete(item *Item) {
	heap.Remove(mh, item.index)
}

func NewMinHeap() *MinHeap {
	mh := make(MinHeap, 0)
	heap.Init(&mh)
	return &mh
}
