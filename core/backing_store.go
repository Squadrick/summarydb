package core

import (
	"github.com/dgraph-io/ristretto"
	"summarydb/protos"
	"summarydb/storage"
	capnp "zombiezen.com/go/capnproto2"
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

type BackingStore struct {
	backend       storage.Backend
	landmarkCache *ristretto.Cache
	summaryCache  *ristretto.Cache
}

func NewBackingStore(backend storage.Backend) *BackingStore {
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

	return &BackingStore{
		backend:       backend,
		landmarkCache: landmarkCache,
		summaryCache:  summaryCache,
	}
}

func (store *BackingStore) Get(streamID, windowID int64) *SummaryWindow {
	window, found := store.summaryCache.Get(storage.GetKey(streamID, windowID))
	if found {
		return window.(*SummaryWindow)
	}
	buf := store.backend.Get(streamID, windowID)
	return BytesToSummaryWindow(buf)
}

func (store *BackingStore) Put(streamID, windowID int64, window *SummaryWindow) {
	store.summaryCache.Set(storage.GetKey(streamID, windowID), window, 1)
	buf := SummaryWindowToBytes(window)
	store.backend.Put(streamID, windowID, buf)
}

func (store *BackingStore) Delete(streamID, windowID int64) {
	store.summaryCache.Del(storage.GetKey(streamID, windowID))
	store.backend.Delete(streamID, windowID)
}

func (store *BackingStore) GetLandmark(streamID, windowID int64) *LandmarkWindow {
	window, found := store.landmarkCache.Get(storage.GetKey(streamID, windowID))
	if found {
		return window.(*LandmarkWindow)
	}
	buf := store.backend.GetLandmark(streamID, windowID)
	return BytesToLandmarkWindow(buf)
}

func (store *BackingStore) PutLandmark(streamID, windowID int64, window *LandmarkWindow) {
	store.landmarkCache.Set(storage.GetKey(streamID, windowID), window, 1)
	buf := LandmarkWindowToBytes(window)
	store.backend.PutLandmark(streamID, windowID, buf)
}

func (store *BackingStore) DeleteLandmark(streamID, windowID int64) {
	store.landmarkCache.Del(storage.GetKey(streamID, windowID))
	store.backend.DeleteLandmark(streamID, windowID)
}
