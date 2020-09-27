package core

import (
	"strconv"
	"summarydb/protos"
	capnp "zombiezen.com/go/capnproto2"
)

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

// --- MAIN MEMORY ---
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

// --- DISK MEMORY ---

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
