package storage

import (
	"encoding/binary"
	"sync"
)

func BitSet(cond bool) byte {
	if cond {
		return 1
	}
	return 0
}

func GetStreamLandmarkSegment(landmark bool, streamID int64) []byte {
	buf := make([]byte, 9)
	binary.LittleEndian.PutUint64(buf[:8], uint64(streamID))
	buf[8] = BitSet(landmark)
	return buf
}

func GetKey(landmark bool, streamID, windowID int64) []byte {
	buf := make([]byte, 17)

	// <8-bits stream ID> <1-bit for landmark> <8-bits for window ID>
	binary.LittleEndian.PutUint64(buf[:8], uint64(streamID))
	buf[8] = BitSet(landmark)
	binary.LittleEndian.PutUint64(buf[9:], uint64(windowID))

	return buf
}

func GetStreamIDFromKey(buf []byte) int64 {
	return int64(binary.LittleEndian.Uint64(buf[:8]))
}

func GetWindowIDFromKey(buf []byte) int64 {
	return int64(binary.LittleEndian.Uint64(buf[9:]))
}

func GetLandmarkFromKey(buf []byte) bool {
	return buf[8] == 1
}

type Backend interface {
	Get(int64, int64) []byte
	Put(int64, int64, []byte)
	Delete(int64, int64)
	Merge(int64, int64, []byte, []int64)

	GetLandmark(int64, int64) []byte
	PutLandmark(int64, int64, []byte)
	DeleteLandmark(int64, int64)

	IterateIndex(int64, func(int64), bool)

	Close()
}

type InMemoryBackend struct {
	summaryMap       map[string][]byte
	landmarkMap      map[string][]byte
	summaryMapMutex  sync.Mutex
	landmarkMapMutex sync.Mutex
}

func NewInMemoryBackend() *InMemoryBackend {
	return &InMemoryBackend{
		summaryMap:  make(map[string][]byte),
		landmarkMap: make(map[string][]byte),
	}
}

func (backend *InMemoryBackend) Get(streamID, windowID int64) []byte {
	backend.summaryMapMutex.Lock()
	defer backend.summaryMapMutex.Unlock()
	return backend.summaryMap[string(GetKey(false, streamID, windowID))]
}

func (backend *InMemoryBackend) Put(streamID, windowID int64, buf []byte) {
	backend.summaryMapMutex.Lock()
	defer backend.summaryMapMutex.Unlock()
	backend.summaryMap[string(GetKey(false, streamID, windowID))] = buf
}

func (backend *InMemoryBackend) Delete(streamID, windowID int64) {
	backend.summaryMapMutex.Lock()
	defer backend.summaryMapMutex.Unlock()
	delete(backend.summaryMap, string(GetKey(false, streamID, windowID)))
}

func (backend *InMemoryBackend) Merge(
	streamID int64,
	windowID int64,
	buf []byte,
	deletedIDs []int64) {
	backend.summaryMapMutex.Lock()
	defer backend.summaryMapMutex.Unlock()

	backend.summaryMap[string(GetKey(false, streamID, windowID))] = buf
	for _, ID := range deletedIDs {
		delete(backend.summaryMap, string(GetKey(false, streamID, ID)))
	}
}

func (backend *InMemoryBackend) GetLandmark(streamID, windowID int64) []byte {
	backend.landmarkMapMutex.Lock()
	defer backend.landmarkMapMutex.Unlock()
	return backend.landmarkMap[string(GetKey(true, streamID, windowID))]
}

func (backend *InMemoryBackend) PutLandmark(streamID, windowID int64, buf []byte) {
	backend.landmarkMapMutex.Lock()
	defer backend.landmarkMapMutex.Unlock()
	backend.landmarkMap[string(GetKey(true, streamID, windowID))] = buf
}

func (backend *InMemoryBackend) DeleteLandmark(streamID, windowID int64) {
	backend.landmarkMapMutex.Lock()
	defer backend.landmarkMapMutex.Unlock()
	delete(backend.landmarkMap, string(GetKey(true, streamID, windowID)))
}

func (backend *InMemoryBackend) Close() {
	backend.summaryMap = nil
	backend.landmarkMap = nil
}

func (backend *InMemoryBackend) IterateIndex(streamID int64, lambda func(int64), landmark bool) {
	var iterMap map[string][]byte
	if landmark {
		iterMap = backend.landmarkMap
	} else {
		iterMap = backend.summaryMap
	}

	for k := range iterMap {
		buf := []byte(k)
		if GetStreamIDFromKey(buf) != streamID {
			continue
		}
		lambda(GetWindowIDFromKey(buf))
	}
}
