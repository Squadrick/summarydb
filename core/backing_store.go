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

func (store *BackingStore) Get(streamID, windowID int64) (*SummaryWindow, error) {
	if store.cacheEnabled {
		window, found := store.summaryCache.Get(storage.GetKey(false, streamID, windowID))
		if found {
			return window.(*SummaryWindow), nil
		}
	}
	buf, err := store.backend.Get(streamID, windowID)
	if err != nil {
		return nil, err
	}
	window, err := BytesToSummaryWindow(buf)
	if err != nil {
		return nil, err
	}
	return window, nil
}

func (store *BackingStore) Put(streamID, windowID int64, window *SummaryWindow) error {
	if store.cacheEnabled {
		store.summaryCache.Set(storage.GetKey(false, streamID, windowID), window, 1)
	}
	buf, err := SummaryWindowToBytes(window)
	if err != nil {
		return err
	}
	return store.backend.Put(streamID, windowID, buf)
}

func (store *BackingStore) Delete(streamID, windowID int64) error {
	if store.cacheEnabled {
		store.summaryCache.Del(storage.GetKey(false, streamID, windowID))
	}
	return store.backend.Delete(streamID, windowID)
}

func (store *BackingStore) MergeWindows(
	streamID int64,
	mergedWindow *SummaryWindow,
	deletedWindowIDs []int64) error {

	if store.cacheEnabled {
		store.summaryCache.Set(
			storage.GetKey(false, streamID, mergedWindow.Id()),
			mergedWindow,
			1)

		for _, swid := range deletedWindowIDs {
			store.summaryCache.Del(storage.GetKey(false, streamID, swid))
		}
	}

	buf, err := SummaryWindowToBytes(mergedWindow)
	if err != nil {
		return err
	}
	return store.backend.Merge(
		streamID, mergedWindow.Id(), buf, deletedWindowIDs)
}

func (store *BackingStore) GetLandmark(streamID, windowID int64) (*LandmarkWindow, error) {
	if store.cacheEnabled {
		window, found := store.landmarkCache.Get(storage.GetKey(true, streamID, windowID))
		if found {
			return window.(*LandmarkWindow), nil
		}
	}
	buf, err := store.backend.GetLandmark(streamID, windowID)
	if err != nil {
		return nil, err
	}
	window, err := BytesToLandmarkWindow(buf)
	if err != nil {
		return nil, err
	}
	return window, nil
}

func (store *BackingStore) PutLandmark(streamID, windowID int64, window *LandmarkWindow) error {
	if store.cacheEnabled {
		store.landmarkCache.Set(storage.GetKey(true, streamID, windowID), window, 1)
	}
	buf, err := LandmarkWindowToBytes(window)
	if err != nil {
		return err
	}
	return store.backend.PutLandmark(streamID, windowID, buf)
}

func (store *BackingStore) DeleteLandmark(streamID, windowID int64) error {
	if store.cacheEnabled {
		store.landmarkCache.Del(storage.GetKey(true, streamID, windowID))
	}
	return store.backend.DeleteLandmark(streamID, windowID)
}

func (store *BackingStore) GetHeap(streamID int64) (*tree.MinHeap, error) {
	rawBytes, err := store.backend.GetHeap(streamID)
	if err != nil {
		return nil, err
	}
	return BytesToHeap(rawBytes)
}

func (store *BackingStore) PutHeap(streamID int64, heap *tree.MinHeap) error {
	rawBytes, err := HeapToBytes(heap)
	if err != nil {
		return err
	}
	return store.backend.PutHeap(streamID, rawBytes)
}

func (store *BackingStore) GetMergerIndex(streamID int64) (*MergerIndex, error) {
	rawBytes, err := store.backend.GetMergerIndex(streamID)
	if err != nil {
		return nil, err
	}
	return BytesToMergerIndex(rawBytes)
}

func (store *BackingStore) PutMergerIndex(streamID int64, index *MergerIndex) error {
	rawBytes, err := MergerIndexToBytes(index)
	if err != nil {
		return err
	}
	return store.backend.PutMergerIndex(streamID, rawBytes)
}

func (store *BackingStore) PutCountAndTime(
	streamID int64,
	compType storage.CompType,
	count int64,
	timestamp int64) error {
	return store.backend.PutCountAndTime(streamID, compType, count, timestamp)
}

func (store *BackingStore) GetCountAndTime(
	streamID int64,
	compType storage.CompType) (int64, int64, error) {
	return store.backend.GetCountAndTime(streamID, compType)
}
