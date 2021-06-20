package core

import (
	"math"
	"summarydb/storage"
	"summarydb/tree"
)

type StreamWindowManager struct {
	id            int64
	summaryIndex  *storage.QueryIndex
	landmarkIndex *storage.QueryIndex
	operators     *OpSet
	backingStore  *BackingStore
}

// TODO: Make this return []*DataTable
func GetDataFromWindows(windows []*SummaryWindow) []DataTable {
	opData := make([]DataTable, len(windows))
	for i, window := range windows {
		opData[i] = *window.Data
	}
	return opData
}

func NewStreamWindowManager(id int64, operatorNames []string) *StreamWindowManager {
	return &StreamWindowManager{
		id:            id,
		summaryIndex:  storage.NewQueryIndex(),
		landmarkIndex: storage.NewQueryIndex(),
		operators:     NewOpSet(operatorNames),
	}
}

func (manager *StreamWindowManager) SetBackingStore(store *BackingStore) {
	manager.backingStore = store
}

func (manager *StreamWindowManager) PrimeUp() error {
	return PopulateBackendIndex(manager.backingStore.backend,
		manager.summaryIndex,
		manager.landmarkIndex,
		manager.id)
}

// SUMMARY WINDOWS

func (manager *StreamWindowManager) MergeSummaryWindows(summaryWindows []*SummaryWindow) *SummaryWindow {
	if len(summaryWindows) == 0 {
		return nil
	}

	firstWindow := summaryWindows[0]
	lastWindow := summaryWindows[len(summaryWindows)-1]

	mergedWindow := NewSummaryWindow(
		firstWindow.TimeStart,
		lastWindow.TimeEnd,
		firstWindow.CountStart,
		lastWindow.CountEnd)

	opData := GetDataFromWindows(summaryWindows)
	mergedWindow.Data = manager.operators.Merge(opData)
	return mergedWindow
}

func (manager *StreamWindowManager) InsertIntoSummaryWindow(window *SummaryWindow, ts int64, value float64) {
	if value == -math.MaxFloat64 {
		return
	}
	manager.operators.Insert(window.Data, value, ts)
}

func (manager *StreamWindowManager) GetSummaryWindow(swid int64) (*SummaryWindow, error) {
	return manager.backingStore.Get(manager.id, swid)
}

func (manager *StreamWindowManager) GetSummaryWindowInRange(t0, t1 int64) ([]*SummaryWindow, error) {
	ids := manager.summaryIndex.GetOverlappingWindowIDs(t0, t1)
	summaryWindows := make([]*SummaryWindow, 0, len(ids))

	for _, id := range ids {
		window, err := manager.GetSummaryWindow(id)
		if err != nil {
			return nil, err
		}
		if window.TimeEnd < t0 || window.TimeStart > t1 {
			continue
		}
		summaryWindows = append(summaryWindows, window)
	}
	return summaryWindows, nil
}

func (manager *StreamWindowManager) PutSummaryWindow(window *SummaryWindow) error {
	manager.summaryIndex.Add(window.Id())
	return manager.backingStore.Put(manager.id, window.Id(), window)
}

func (manager *StreamWindowManager) DeleteSummaryWindow(swid int64) error {
	manager.summaryIndex.Remove(swid)
	return manager.backingStore.Delete(manager.id, swid)
}

func (manager *StreamWindowManager) NumSummaryWindows() int {
	return manager.summaryIndex.GetNumberWindows()
}

// LANDMARK WINDOWS

func (manager *StreamWindowManager) GetLandmarkWindow(lwid int64) (*LandmarkWindow, error) {
	return manager.backingStore.GetLandmark(manager.id, lwid)
}

func (manager *StreamWindowManager) GetLandmarkWindowInRange(t0, t1 int64) ([]*LandmarkWindow, error) {
	ids := manager.landmarkIndex.GetOverlappingWindowIDs(t0, t1)
	landmarkWindows := make([]*LandmarkWindow, 0, len(ids))

	for _, id := range ids {
		window, err := manager.GetLandmarkWindow(id)
		if err != nil {
			return nil, err
		}
		if window.TimeEnd < t0 {
			continue
		}
		landmarkWindows = append(landmarkWindows, window)
	}
	return landmarkWindows, nil
}

func (manager *StreamWindowManager) PutLandmarkWindow(window *LandmarkWindow) error {
	manager.landmarkIndex.Add(window.Id())
	return manager.backingStore.PutLandmark(manager.id, window.Id(), window)
}

func (manager *StreamWindowManager) DeleteLandmarkWindow(swid int64) error {
	manager.landmarkIndex.Remove(swid)
	return manager.backingStore.DeleteLandmark(manager.id, swid)
}

func (manager *StreamWindowManager) NumLandmarkWindows() int {
	return manager.landmarkIndex.GetNumberWindows()
}

func (manager *StreamWindowManager) GetHeap() (*tree.MinHeap, error) {
	return manager.backingStore.GetHeap(manager.id)
}

func (manager *StreamWindowManager) GetMergerIndex() (*MergerIndex, error) {
	return manager.backingStore.GetMergerIndex(manager.id)
}

func (manager *StreamWindowManager) PutCountAndTime(
	compType storage.CompType,
	count int64,
	timestamp int64) error {
	return manager.backingStore.PutCountAndTime(manager.id, compType, count, timestamp)
}

func (manager *StreamWindowManager) GetCountAndTime(
	compType storage.CompType) (int64, int64, error) {
	return manager.backingStore.GetCountAndTime(manager.id, compType)
}

// --- special brews ---

func (manager *StreamWindowManager) WriterBrew(
	count int64, timestamp int64, window *SummaryWindow) error {
	manager.summaryIndex.Add(window.Id())
	return manager.backingStore.WriterBrew(
		manager.id, count, timestamp, window.Id(), window)
}

func (manager *StreamWindowManager) MergerBrew(
	count int64, timestamp int64,
	pendingMerges []*PendingMerge,
	heap *tree.MinHeap, index *MergerIndex) error {

	for _, pm := range pendingMerges {
		manager.summaryIndex.Add(pm.MergedWindow.Id())
		for _, swid := range pm.DeletedIDs {
			manager.summaryIndex.Remove(swid)
		}
	}

	return manager.backingStore.MergerBrew(
		manager.id, count, timestamp,
		pendingMerges, heap, index)
}
