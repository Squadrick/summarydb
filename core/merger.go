package core

import (
	"container/heap"
	"context"
	"summarydb/tree"
	"summarydb/window"
	"sync"
)

const bufferSize int = 100

type MergeEvent struct {
	Id   int64
	Size int64
}

var shutdownMergeEvent *MergeEvent = nil
var shutdownMergeMutex sync.Mutex
var flushMergeEvent *MergeEvent = nil
var flushMergeMutex sync.Mutex

func ConstShutdownMergeEvent() *MergeEvent {
	shutdownMergeMutex.Lock()
	defer shutdownMergeMutex.Unlock()
	if shutdownMergeEvent == nil {
		shutdownMergeEvent = &MergeEvent{-1, -1}
	}
	return shutdownMergeEvent
}

func ConstFlushMergeEvent() *MergeEvent {
	flushMergeMutex.Lock()
	defer flushMergeMutex.Unlock()
	if flushMergeEvent == nil {
		flushMergeEvent = &MergeEvent{-1, -1}
	}
	return flushMergeEvent
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
	mutex               sync.Mutex
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
		mutex:               sync.Mutex{},
	}
}

func (hm *Merger) SetWindowManager(manager *StreamWindowManager) {
	hm.streamWindowManager = manager
}

func (hm *Merger) PrimeUp() {
	if hm.streamWindowManager == nil {
		panic("cannot prime without window manager")
	}
	hm.mergeCounts = hm.streamWindowManager.GetHeap()
	hm.index.PopulateFromHeap(hm.mergeCounts)
}

// Given consecutive windows w0, w1 which together span the count [c0, c1],
// set mergeCounts[w] = first n' >= n such that (w, w_next) will need to
// merged after n' elements have been inserted.
func (hm *Merger) updateMergeCountFor(w, c0, c1, n int64) {
	if w == InvalidInt64 || c0 == InvalidInt64 || c1 == InvalidInt64 {
		return
	}

	existingEntry := hm.index.UnsetHeapItem(w)
	if existingEntry != nil {
		heap.Remove(hm.mergeCounts, existingEntry.Index)
	}

	newMergeCount, ok := hm.windowing.GetFirstContainingTime(c0, c1, n)

	if ok {
		item := &tree.HeapItem{
			Value:    w,
			Priority: int(newMergeCount),
			Index:    -1,
		}
		heap.Push(hm.mergeCounts, item)
		hm.index.SetHeapItem(w, item)
	}
}

func (hm *Merger) GetNumUnissuedMerges() int {
	return len(hm.pendingMerges)
}

// HANDLING MERGING

// Add entry merge(w0, [windows merged into w0], w1, [windows merged into w1])
func (hm *Merger) addPendingMerge(w0 int64, w1 int64) {
	if w0 == InvalidInt64 || w1 == InvalidInt64 {
		return
	}
	tail, found := hm.pendingMerges[w0]
	if !found {
		tail = make([]int64, 0)
	}
	tail = append(tail, w1)

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

	// This writes the updated windows to cache/index/disk in a single commit.
	hm.streamWindowManager.UpdateMergeSummaryWindows(mergedWindow, tail)
}

func (hm *Merger) writeHeapToDisk() {
	if hm.streamWindowManager != nil {
		hm.streamWindowManager.PutHeap(hm.mergeCounts)
	}
}

func (hm *Merger) issueAllPendingMerges() {
	var wg sync.WaitGroup

	// NOTE: This is only safe because `issuePendingMerge()` does not
	// access the heap (mergeCounts). If that changes in the future,
	// these two operations cannot be parallelized.
	wg.Add(1)
	go func() {
		hm.writeHeapToDisk()
		wg.Done()
	}()

	for head, tail := range hm.pendingMerges {
		wg.Add(1)
		go func(h int64, t []int64) {
			hm.issuePendingMerge(h, t)
			wg.Done()
		}(head, tail)
	}
	wg.Wait()

	// clear pending merges
	hm.pendingMerges = make(map[int64][]int64)
}

func (hm *Merger) updatePendingMerges() {
	for hm.mergeCounts.Len() != 0 &&
		hm.mergeCounts.Top().(*tree.HeapItem).Priority <= int(hm.numElements) {

		minItem := heap.Pop(hm.mergeCounts).(*tree.HeapItem)
		hm.index.UnsetHeapItem(minItem.Value)

		w1 := minItem.Value
		w2 := hm.index.GetSucc(w1)

		w0 := hm.index.GetPred(w1)
		w3 := hm.index.GetSucc(w2)

		w1NewStart := hm.index.GetCStart(w1)
		w1NewEnd := hm.index.GetCEnd(w2)

		hm.addPendingMerge(w1, w2)
		w2RemovedIndexItem := hm.index.Remove(w2)
		hm.index.Put(w1, w1NewEnd)

		if w2RemovedIndexItem != nil && w2RemovedIndexItem.heapItem != nil {
			heap.Remove(hm.mergeCounts, w2RemovedIndexItem.heapItem.Index)
		}

		w0Start := hm.index.GetCStart(w0)
		w3End := hm.index.GetCEnd(w3)

		hm.updateMergeCountFor(w0, w0Start, w1NewEnd, hm.numElements)
		hm.updateMergeCountFor(w1, w1NewStart, w3End, hm.numElements)
	}
}

func (hm *Merger) Process(mergeEvent *MergeEvent) {
	hm.mutex.Lock()
	defer hm.mutex.Unlock()
	hm.numElements += mergeEvent.Size
	hm.numWindows += 1

	lastWindowId := hm.index.GetLastSWID()
	cStart := hm.index.GetCStart(lastWindowId)
	if lastWindowId != InvalidInt64 {
		hm.updateMergeCountFor(lastWindowId, cStart, hm.numElements-1, hm.numElements)
	}

	hm.index.Put(mergeEvent.Id, hm.numElements-1)
	hm.updatePendingMerges()
	if hm.numWindows%hm.windowsPerBatch == 0 {
		hm.issueAllPendingMerges()
	}
}

func (hm *Merger) flush() {
	hm.mutex.Lock()
	defer hm.mutex.Unlock()
	hm.issueAllPendingMerges()
	if hm.barrier != nil {
		hm.barrier.Notify(MERGER)
	}
}

func (hm *Merger) Run(ctx context.Context, inputCh <-chan *MergeEvent) {
	for {
		select {
		case mergeEvent := <-inputCh:
			if mergeEvent == ConstShutdownMergeEvent() {
				hm.flush()
				break
			} else if mergeEvent == ConstFlushMergeEvent() {
				hm.flush()
				continue
			} else {
				hm.Process(mergeEvent)
			}

		case <-ctx.Done():
			// done running this loop
			return
		}
	}
}
