package core

import (
	"github.com/dgraph-io/ristretto"
	"summarydb/storage"
	"summarydb/tree"
)

// TODO: Support transactions in BackingStore. The API will as follows:
//		txn := backingStore.BeginTransaction()
//		txn.Put(...)
//		txn.PutLandmark(...)
//		...
//		txn.Close()
type BackingStore struct {
	backend       storage.Backend
	cacheEnabled  bool
	landmarkCache *ristretto.Cache
	summaryCache  *ristretto.Cache
}

func NewBackingStore(backend storage.Backend, cacheEnabled bool) *BackingStore {
	landmarkCache, _ := ristretto.NewCache(&ristretto.Config{
		NumCounters: 1e3,
		MaxCost:     1 << 25,
		BufferItems: 64,
	})
	summaryCache, _ := ristretto.NewCache(&ristretto.Config{
		NumCounters: 1e6,
		MaxCost:     1 << 28,
		BufferItems: 64,
	})
	// TODO: Cache for heap?

	return &BackingStore{
		backend:       backend,
		cacheEnabled:  cacheEnabled,
		landmarkCache: landmarkCache,
		summaryCache:  summaryCache,
	}
}

func (store *BackingStore) Get(streamID, windowID int64) *SummaryWindow {
	if store.cacheEnabled {
		window, found := store.summaryCache.Get(storage.GetKey(false, streamID, windowID))
		if found {
			return window.(*SummaryWindow)
		}
	}
	buf := store.backend.Get(streamID, windowID)
	return BytesToSummaryWindow(buf)
}

func (store *BackingStore) Put(streamID, windowID int64, window *SummaryWindow) {
	if store.cacheEnabled {
		store.summaryCache.Set(storage.GetKey(false, streamID, windowID), window, 1)
	}
	buf := SummaryWindowToBytes(window)
	store.backend.Put(streamID, windowID, buf)
}

func (store *BackingStore) Delete(streamID, windowID int64) {
	if store.cacheEnabled {
		store.summaryCache.Del(storage.GetKey(false, streamID, windowID))
	}
	store.backend.Delete(streamID, windowID)
}

func (store *BackingStore) MergeWindows(
	streamID int64,
	mergedWindow *SummaryWindow,
	deletedWindowIDs []int64) {

	if store.cacheEnabled {
		store.summaryCache.Set(
			storage.GetKey(false, streamID, mergedWindow.Id()),
			mergedWindow,
			1)

		for _, swid := range deletedWindowIDs {
			store.summaryCache.Del(storage.GetKey(false, streamID, swid))
		}
	}

	buf := SummaryWindowToBytes(mergedWindow)
	store.backend.Merge(
		streamID, mergedWindow.Id(), buf, deletedWindowIDs)
}

func (store *BackingStore) GetLandmark(streamID, windowID int64) *LandmarkWindow {
	if store.cacheEnabled {
		window, found := store.landmarkCache.Get(storage.GetKey(true, streamID, windowID))
		if found {
			return window.(*LandmarkWindow)
		}
	}
	buf := store.backend.GetLandmark(streamID, windowID)
	return BytesToLandmarkWindow(buf)
}

func (store *BackingStore) PutLandmark(streamID, windowID int64, window *LandmarkWindow) {
	if store.cacheEnabled {
		store.landmarkCache.Set(storage.GetKey(true, streamID, windowID), window, 1)
	}
	buf := LandmarkWindowToBytes(window)
	store.backend.PutLandmark(streamID, windowID, buf)
}

func (store *BackingStore) DeleteLandmark(streamID, windowID int64) {
	if store.cacheEnabled {
		store.landmarkCache.Del(storage.GetKey(true, streamID, windowID))
	}
	store.backend.DeleteLandmark(streamID, windowID)
}

func (store *BackingStore) GetHeap(streamID int64) *tree.MinHeap {
	rawBytes := store.backend.GetHeap(streamID)
	return BytesToHeap(rawBytes)
}

func (store *BackingStore) PutHeap(streamID int64, heap *tree.MinHeap) {
	rawBytes := HeapToBytes(heap)
	store.backend.PutHeap(streamID, rawBytes)
}

func (store *BackingStore) GetMergerIndex(streamID int64) *MergerIndex {
	rawBytes := store.backend.GetMergerIndex(streamID)
	return BytesToMergerIndex(rawBytes)
}

func (store *BackingStore) PutMergerIndex(streamID int64, index *MergerIndex) {
	rawBytes := MergerIndexToBytes(index)
	store.backend.PutMergerIndex(streamID, rawBytes)
}

func (store *BackingStore) PutCountAndTime(
	streamID int64,
	compType storage.CompType,
	count int64,
	timestamp int64) {
	store.backend.PutCountAndTime(streamID, compType, count, timestamp)
}

func (store *BackingStore) GetCountAndTime(
	streamID int64,
	compType storage.CompType) (int64, int64) {
	return store.backend.GetCountAndTime(streamID, compType)
}
