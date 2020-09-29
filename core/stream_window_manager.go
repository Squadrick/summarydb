package core

import (
	"math"
	"summarydb/storage"
)

type StreamWindowManager struct {
	id            int64
	summaryIndex  *storage.QueryIndex
	landmarkIndex *storage.QueryIndex
	operators     *OpSet
	backingStore  *BackingStore
}

func GetDataFromWindows(windows []SummaryWindow) []DataTable {
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

// SUMMARY WINDOWS

func (manager *StreamWindowManager) MergeSummaryWindows(summaryWindows []SummaryWindow) {
	if len(summaryWindows) == 0 {
		return
	}

	summaryWindows[0].CountEnd = summaryWindows[len(summaryWindows)-1].CountEnd
	summaryWindows[0].TimeEnd = summaryWindows[len(summaryWindows)-1].TimeEnd

	opData := GetDataFromWindows(summaryWindows)
	summaryWindows[0].Data = manager.operators.Merge(opData)
}

func (manager *StreamWindowManager) InsertIntoSummaryWindow(window *SummaryWindow, ts int64, value float64) {
	if value == -math.MaxFloat64 {
		return
	}
	manager.operators.Insert(window.Data, value, ts)
}

func (manager *StreamWindowManager) GetSummaryWindow(swid int64) *SummaryWindow {
	return manager.backingStore.Get(manager.id, swid)
}

func (manager *StreamWindowManager) GetSummaryWindowInRange(t0, t1 int64) []SummaryWindow {
	ids := manager.summaryIndex.GetOverlappingWindowIDs(t0, t1)
	summaryWindows := make([]SummaryWindow, 0, len(ids))

	for _, id := range ids {
		window := manager.GetSummaryWindow(id)
		if window.TimeEnd < t0 || window.TimeStart > t1 {
			continue
		}
		summaryWindows = append(summaryWindows, *manager.GetSummaryWindow(id))
	}
	return summaryWindows
}

func (manager *StreamWindowManager) PutSummaryWindow(window *SummaryWindow) {
	manager.summaryIndex.Add(window.Id())
	manager.backingStore.Put(manager.id, window.Id(), window)
}

func (manager *StreamWindowManager) DeleteSummaryWindow(swid int64) {
	manager.summaryIndex.Remove(swid)
	manager.backingStore.Delete(manager.id, swid)
}

func (manager *StreamWindowManager) NumSummaryWindows() int {
	return manager.summaryIndex.GetNumberWindows()
}

// LANDMARK WINDOWS

func (manager *StreamWindowManager) GetLandmarkWindow(lwid int64) *LandmarkWindow {
	return manager.backingStore.GetLandmark(manager.id, lwid)
}

func (manager *StreamWindowManager) GetLandmarkWindowInRange(t0, t1 int64) []LandmarkWindow {
	ids := manager.landmarkIndex.GetOverlappingWindowIDs(t0, t1)
	landmarkWindows := make([]LandmarkWindow, 0, len(ids))

	for _, id := range ids {
		window := manager.GetLandmarkWindow(id)
		if window.TimeEnd < t0 {
			continue
		}
		landmarkWindows = append(landmarkWindows, *manager.GetLandmarkWindow(id))
	}
	return landmarkWindows
}

func (manager *StreamWindowManager) PutLandmarkWindow(window *LandmarkWindow) {
	manager.landmarkIndex.Add(window.Id())
	manager.backingStore.PutLandmark(manager.id, window.Id(), window)
}

func (manager *StreamWindowManager) DeleteLandmarkWindow(swid int64) {
	manager.landmarkIndex.Remove(swid)
	manager.backingStore.DeleteLandmark(manager.id, swid)
}

func (manager *StreamWindowManager) NumLandmarkWindows() int {
	return manager.landmarkIndex.GetNumberWindows()
}
