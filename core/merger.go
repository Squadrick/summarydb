package core

import (
	"context"
	"fmt"
	"summarydb/tree"
	"summarydb/window"
)

const bufferSize int = 100

type MergeEvent struct {
	Id   int64
	Size int64
}

var shutdownMergeEvent *MergeEvent = nil
var flushMergeEvent *MergeEvent = nil

func ConstShutdownMergeEvent() *MergeEvent {
	if shutdownMergeEvent == nil {
		shutdownMergeEvent = &MergeEvent{-1, -1}
	}
	return shutdownMergeEvent
}

func ConstFlushMergeEvent() *MergeEvent {
	if flushMergeEvent == nil {
		flushMergeEvent = &MergeEvent{-1, -1}
	}
	return flushMergeEvent
}

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

func (event *MergerIndex) PopulateFromHeap(heap *tree.MinHeap) {
	for _, entry := range *heap {
		int64Key := tree.Int64Key(entry.Value)
		item, ok := event.indexMap.Get(&int64Key)
		if !ok {
			continue
		}
		indexItem := item.(*MergerIndexItem)
		indexItem.heapItem = entry
	}
}

func (event *MergerIndex) Put(swid int64, cEnd int64) {
	key := tree.Int64Key(swid)
	item := &MergerIndexItem{
		cEnd:     cEnd,
		heapItem: nil,
	}
	event.indexMap.Insert(&key, item)
}

func (event *MergerIndex) Remove(swid int64) *MergerIndexItem {
	key := tree.Int64Key(swid)
	item, ok := event.indexMap.Get(&key)
	if !ok {
		return nil
	}
	indexItem := item.(*MergerIndexItem)
	event.indexMap.Delete(&key)
	return indexItem
}

func (event *MergerIndex) Contains(swid int64) bool {
	key := tree.Int64Key(swid)
	return event.indexMap.Exists(&key)
}

func (event *MergerIndex) GetCStart(swid int64) (int64, bool) {
	if !event.Contains(swid) {
		return -1, false
	}
	key := tree.Int64Key(swid)
	_, prevItem := event.indexMap.Lower(&key)
	if prevItem == nil {
		return 0, true
	}
	indexItem := prevItem.(*MergerIndexItem)
	return indexItem.cEnd + 1, true
}

func (event *MergerIndex) GetCEnd(swid int64) (int64, bool) {
	key := tree.Int64Key(swid)
	item, ok := event.indexMap.Get(&key)
	if !ok {
		return 0, false
	}
	indexItem := item.(*MergerIndexItem)
	return indexItem.cEnd, true
}

func (event *MergerIndex) GetPred(swid int64) (int64, bool) {
	if !event.Contains(swid) {
		return 0, false
	}
	key := tree.Int64Key(swid)
	prevKey, _ := event.indexMap.Lower(&key)
	if prevKey == nil {
		return 0, false
	}
	return int64(*prevKey.(*tree.Int64Key)), true
}

func (event *MergerIndex) GetSucc(swid int64) (int64, bool) {
	if !event.Contains(swid) {
		return 0, false
	}
	key := tree.Int64Key(swid)
	succKey, _ := event.indexMap.Higher(&key)
	if succKey == nil {
		return 0, false
	}
	return int64(*succKey.(*tree.Int64Key)), true
}

func (event *MergerIndex) GetLastSWID() (int64, bool) {
	if event.indexMap.IsEmpty() {
		return 0, false
	}

	maxKey, _ := event.indexMap.Max()
	return int64(*maxKey.(*tree.Int64Key)), true
}

func (event *MergerIndex) UnsetHeapItem(swid int64) *tree.HeapItem {
	key := tree.Int64Key(swid)
	item, ok := event.indexMap.Get(&key)
	if !ok {
		return nil
	}
	indexItem := item.(*MergerIndexItem)
	heapPtr := indexItem.heapItem
	indexItem.heapItem = nil
	return heapPtr
}

func (event *MergerIndex) SetHeapItem(swid int64, heapItem *tree.HeapItem) bool {
	key := tree.Int64Key(swid)
	item, ok := event.indexMap.Get(&key)
	if !ok {
		return ok
	}
	indexItem := item.(*MergerIndexItem)
	indexItem.heapItem = heapItem
	return true
}

// mergeCounts is a priority queue, mapping each summary window w_i to the time
// at which w_{i+1} will be merged into it.
type Merger struct {
	streamWindowManager *StreamWindowManager
	windowing           window.Windowing
	windowsPerBatch     int64
	mergeCounts         *tree.MinHeap
	index               *MergerIndex
	numElements         int64
	numWindows          int64
	pendingMerges       map[int64][]int64
	barrier             *Barrier
}

func NewMerger(windowing window.Windowing, windowsPerBatch int64, barrier *Barrier) *Merger {
	return &Merger{
		streamWindowManager: nil,
		windowing:           windowing,
		windowsPerBatch:     windowsPerBatch,
		mergeCounts:         tree.NewMinHeap(bufferSize),
		index:               NewMergerIndex(),
		numWindows:          0,
		numElements:         0,
		pendingMerges:       make(map[int64][]int64),
		barrier:             barrier,
	}
}

func (hm *Merger) SetWindowManager(manager *StreamWindowManager) {
	hm.streamWindowManager = manager
	hm.index.PopulateFromHeap(hm.mergeCounts)
}

// Given consecutive windows w0, w1 which together span the count [c0, c1],
// set mergeCounts[(w0, w1)] = first n' >= n such that (w0, w1) will need to
// merged after n' elements have been inserted.
func (hm *Merger) updateMergeCountFor(w0, w1, c0, c1, n int64) {
	existingEntry := hm.index.UnsetHeapItem(w0)
	if existingEntry != nil {
		hm.mergeCounts.Delete(existingEntry)
	}

	newMergeCount, ok := hm.windowing.GetFirstContainingTime(c0, c1, n)

	if ok {
		item := &tree.HeapItem{
			Value:    w0,
			Priority: int(newMergeCount),
			Index:    0,
		}
		hm.mergeCounts.Push(item) // this will set Index in item
		hm.index.SetHeapItem(w0, item)
	}
}

func (hm *Merger) GetNumUnissuedMerges() int {
	return len(hm.pendingMerges)
}

// HANDLING MERGING

// Add entry merge(w0, [windows merged into w0], w1, [windows merged into w1])
func (hm *Merger) addPendingMerge(w0 int64, w1 int64) {
	tail, found := hm.pendingMerges[w0]
	if !found {
		tail = make([]int64, 0)
	}
	tail = append(tail, w0)

	transitiveTail, found := hm.pendingMerges[w1]
	if found {
		for _, val := range transitiveTail {
			tail = append(tail, val)
		}
		delete(hm.pendingMerges, w1)
	}
	hm.pendingMerges[w0] = tail
}

func (hm *Merger) issuePendingMerge(head int64, tail []int64) {
	if tail == nil || len(tail) == 0 {
		return
	}
	// mergeLengthStats.addValue(1 + len(tail))
	windows := make([]*SummaryWindow, 0, len(tail)+1)
	windows = append(windows, hm.streamWindowManager.GetSummaryWindow(head))

	for _, swid := range tail {
		windows = append(windows, hm.streamWindowManager.GetSummaryWindow(swid))
	}

	mergedWindow := hm.streamWindowManager.MergeSummaryWindows(windows)
	hm.streamWindowManager.PutSummaryWindow(mergedWindow)

	for _, swid := range tail {
		// `head` already exists in storage, because the storage key
		// for `mergedWindow` and `head` is the same (TimeStart).
		hm.streamWindowManager.DeleteSummaryWindow(swid)
	}
}

func (hm *Merger) issueAllPendingMerges() {
	// TODO: Parallelize this using a worker pool, let num workers be a param.
	for head, tail := range hm.pendingMerges {
		hm.issuePendingMerge(head, tail)
	}

	// clear pending merges
	hm.pendingMerges = make(map[int64][]int64)
}

func (hm *Merger) updatePendingMerges() {
	for {
		if hm.mergeCounts.Len() == 0 {
			return
		}
		minItem := hm.mergeCounts.Top().(*tree.HeapItem)
		if int64(minItem.Priority) > hm.numElements {
			return
		}
		minItem = hm.mergeCounts.Pop().(*tree.HeapItem)
		hm.index.UnsetHeapItem(minItem.Value)

		w1 := minItem.Value
		w0, ok0 := hm.index.GetPred(w1)
		w2, ok2 := hm.index.GetSucc(w1)
		w3, ok3 := hm.index.GetSucc(w2)

		if !(ok0 && ok2 && ok3) {
			return
		}

		newW1CStart, _ := hm.index.GetCStart(w1)
		newW1CEnd, _ := hm.index.GetCEnd(w2)

		hm.addPendingMerge(w1, w2)

		oldW2IndexItem := hm.index.Remove(w2)
		hm.index.Put(w1, newW1CEnd)

		if oldW2IndexItem.heapItem != nil {
			hm.mergeCounts.Delete(oldW2IndexItem.heapItem)
		}

		w0CStart, ok := hm.index.GetCStart(w0)
		w3CEnd, _ := hm.index.GetCEnd(w3)
		if ok {
			return
		}

		hm.updateMergeCountFor(w0, w1, w0CStart, newW1CEnd, hm.numElements)
		hm.updateMergeCountFor(w0, w3, newW1CStart, w3CEnd, hm.numElements)
	}
}

func (hm *Merger) Run(ctx context.Context, inputCh <-chan *MergeEvent) {
	for {
		select {

		case windowInfo := <-inputCh:
			if windowInfo == ConstShutdownMergeEvent() {
				hm.issueAllPendingMerges()
				if hm.barrier != nil {
					hm.barrier.Notify(MERGER)
				}
				return
			} else if windowInfo == ConstFlushMergeEvent() {
				hm.issueAllPendingMerges()
				if hm.barrier != nil {
					hm.barrier.Notify(MERGER)
				}
				continue
			} else {
				hm.numElements += windowInfo.Size
				hm.numWindows += 1

				lastWindowId, ok := hm.index.GetLastSWID()
				if ok {
					cStart, ok := hm.index.GetCStart(lastWindowId)
					if ok {
						hm.updateMergeCountFor(lastWindowId, windowInfo.Id, cStart, hm.numElements-1, hm.numElements)
					}
				}
				hm.index.Put(windowInfo.Id, hm.numElements-1)
				hm.updatePendingMerges()
				if hm.numWindows%hm.windowsPerBatch == 0 {
					hm.issueAllPendingMerges()
				}
			}

		case <-ctx.Done():
			fmt.Println("ctx cancelled")
			// done running this loop
			return
		}
	}
}
