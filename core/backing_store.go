package core

import (
	"bytes"
	capnp "capnproto.org/go/capnp/v3"
	"encoding/gob"
	_ "encoding/gob"
	"github.com/dgraph-io/ristretto"
	"summarydb/protos"
	"summarydb/storage"
	"summarydb/tree"
)

func SummaryWindowToBytes(window *SummaryWindow) []byte {
	msg, seg, err := capnp.NewMessage(capnp.SingleSegment(nil))

	summaryWindowProto, err := protos.NewRootProtoSummaryWindow(seg)
	if err != nil {
		return nil
	}

	summaryWindowProto.SetTs(window.TimeStart)
	summaryWindowProto.SetTe(window.TimeEnd)
	summaryWindowProto.SetCs(window.CountStart)
	summaryWindowProto.SetCe(window.CountEnd)

	dataTableProto, err := summaryWindowProto.NewOpData()
	if err != nil {
		return nil
	}

	dataTableProto.SetCount(window.Data.Count.Value)
	dataTableProto.SetMax(window.Data.Max.Value)
	dataTableProto.SetSum(window.Data.Sum.Value)

	buf, err := msg.Marshal()
	if err != nil {
		return nil
	}

	return buf
}

func BytesToSummaryWindow(buf []byte) *SummaryWindow {
	msg, err := capnp.Unmarshal(buf)
	if err != nil {
		return nil
	}

	summaryWindowProto, err := protos.ReadRootProtoSummaryWindow(msg)
	if err != nil {
		return nil
	}

	summaryWindow := NewSummaryWindow(
		summaryWindowProto.Ts(),
		summaryWindowProto.Te(),
		summaryWindowProto.Cs(),
		summaryWindowProto.Ce())
	dataTableProto, err := summaryWindowProto.OpData()
	if err != nil {
		return summaryWindow
	}

	summaryWindow.Data.Sum.Value = dataTableProto.Sum()
	summaryWindow.Data.Count.Value = dataTableProto.Count()
	summaryWindow.Data.Max.Value = dataTableProto.Max()
	return summaryWindow
}

func LandmarkWindowToBytes(window *LandmarkWindow) []byte {
	msg, seg, err := capnp.NewMessage(capnp.SingleSegment(nil))

	landmarkWindowProto, err := protos.NewRootProtoLandmarkWindow(seg)
	if err != nil {
		return nil
	}

	landmarkWindowProto.SetTs(window.TimeStart)
	landmarkWindowProto.SetTe(window.TimeEnd)

	timestampsProto, _ := landmarkWindowProto.NewTimestamps(int32(len(window.Landmarks)))
	valuesProto, _ := landmarkWindowProto.NewValues(int32(len(window.Landmarks)))

	for i, landmark := range window.Landmarks {
		timestampsProto.Set(i, landmark.Timestamp)
		valuesProto.Set(i, landmark.Value)
	}

	buf, err := msg.Marshal()
	if err != nil {
		return nil
	}

	return buf
}

func BytesToLandmarkWindow(buf []byte) *LandmarkWindow {
	msg, err := capnp.Unmarshal(buf)
	if err != nil {
		return nil
	}
	landmarkWindowProto, err := protos.ReadRootProtoLandmarkWindow(msg)
	if err != nil {
		return nil
	}

	landmarkWindow := NewLandmarkWindow(landmarkWindowProto.Ts())
	timestampsProto, _ := landmarkWindowProto.Timestamps()
	valuesProto, _ := landmarkWindowProto.Values()

	for i := 0; i < valuesProto.Len(); i++ {
		landmarkWindow.Insert(timestampsProto.At(i), valuesProto.At(i))
	}

	landmarkWindow.Close(landmarkWindowProto.Te())
	return landmarkWindow
}

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

// TODO: Implement alternate versions that use CapnProto. Might be faster.
func HeapToBytes(heap *tree.MinHeap) []byte {
	var buffer bytes.Buffer
	enc := gob.NewEncoder(&buffer)
	err := enc.Encode(*heap)
	if err != nil {
		panic(err)
	}
	return buffer.Bytes()
}

func BytesToHeap(rawBytes []byte) *tree.MinHeap {
	var buffer bytes.Buffer
	buffer.Write(rawBytes)
	dec := gob.NewDecoder(&buffer)
	var heap tree.MinHeap
	err := dec.Decode(&heap)
	if err != nil {
		panic(err)
	}
	return &heap
}

func (store *BackingStore) GetHeap(streamID int64) *tree.MinHeap {
	rawBytes := store.backend.GetHeap(streamID)
	return BytesToHeap(rawBytes)
}

func (store *BackingStore) PutHeap(streamID int64, heap *tree.MinHeap) {
	rawBytes := HeapToBytes(heap)
	store.backend.PutHeap(streamID, rawBytes)
}
