package core

type BackingStore struct {
	// populate info
}

func (store *BackingStore) Get(streamID, windowID int64) *SummaryWindow {
	return nil
}

func (store *BackingStore) Put(streamID, windowID int64, window *SummaryWindow) {
	return
}

func (store *BackingStore) Delete(streamID, windowID int64) {
	return
}

func (store *BackingStore) GetLandmark(streamID, windowID int64) *LandmarkWindow {
	return nil
}

func (store *BackingStore) PutLandmark(streamID, windowID int64, window *LandmarkWindow) {
	return
}

func (store *BackingStore) DeleteLandmark(streamID, windowID int64) {
	return
}
