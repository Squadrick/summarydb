package core

import "strconv"

type BackingStore interface {
	Get(int64, int64) *SummaryWindow
	Put(int64, int64, *SummaryWindow)
	Delete(int64, int64)

	GetLandmark(int64, int64) *LandmarkWindow
	PutLandmark(int64, int64, *LandmarkWindow)
	DeleteLandmark(int64, int64)
}

func GetKey(a, b int64) string {
	return strconv.Itoa(int(a)) + "-" + strconv.Itoa(int(b))
}

type MainMemoryBackingStore struct {
	// populate info
	summaryMap  map[string]*SummaryWindow
	landmarkMap map[string]*LandmarkWindow
}

func NewMainMemoryBackingStore() *MainMemoryBackingStore {
	return &MainMemoryBackingStore{
		summaryMap:  make(map[string]*SummaryWindow),
		landmarkMap: make(map[string]*LandmarkWindow),
	}
}

func (store *MainMemoryBackingStore) Get(streamID, windowID int64) *SummaryWindow {
	return store.summaryMap[GetKey(streamID, windowID)]
}

func (store *MainMemoryBackingStore) Put(streamID, windowID int64, window *SummaryWindow) {
	store.summaryMap[GetKey(streamID, windowID)] = window
}

func (store *MainMemoryBackingStore) Delete(streamID, windowID int64) {
	delete(store.summaryMap, GetKey(streamID, windowID))
}

func (store *MainMemoryBackingStore) GetLandmark(streamID, windowID int64) *LandmarkWindow {
	return store.landmarkMap[GetKey(streamID, windowID)]
}

func (store *MainMemoryBackingStore) PutLandmark(streamID, windowID int64, window *LandmarkWindow) {
	store.landmarkMap[GetKey(streamID, windowID)] = window
}

func (store *MainMemoryBackingStore) DeleteLandmark(streamID, windowID int64) {
	delete(store.landmarkMap, GetKey(streamID, windowID))
}
