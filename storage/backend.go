package storage

import (
	"encoding/binary"
	"errors"
	"strconv"
	"sync"
)

type CompType int

const (
	Pipeline CompType = iota
	Writer   CompType = iota
	Merger   CompType = iota
)

func TwoInt64ToByte128(a int64, b int64) []byte {
	buf := make([]byte, 16)
	binary.LittleEndian.PutUint64(buf[:8], uint64(a))
	binary.LittleEndian.PutUint64(buf[8:], uint64(b))
	return buf
}

func Byte128ToTwoInt64(buf []byte) (int64, int64) {
	return int64(binary.LittleEndian.Uint64(buf[:8])),
		int64(binary.LittleEndian.Uint64(buf[8:]))
}

func BitSet(cond bool) byte {
	if cond {
		return 1
	}
	return 0
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
	Get(int64, int64) ([]byte, error)
	Put(int64, int64, []byte) error
	Delete(int64, int64) error
	Merge(int64, int64, []byte, []int64) error

	GetLandmark(int64, int64) ([]byte, error)
	PutLandmark(int64, int64, []byte) error
	DeleteLandmark(int64, int64) error

	GetHeap(int64) ([]byte, error)
	PutHeap(int64, []byte) error

	GetMergerIndex(int64) ([]byte, error)
	PutMergerIndex(int64, []byte) error

	PutCountAndTime(int64, CompType, int64, int64) error
	GetCountAndTime(int64, CompType) (int64, int64, error)

	IterateIndex(int64, func(int64) error, bool) error

	Close() error

	// --- special atomic functions ---
	// See InMemoryBackend for a basic implementation.

	WriterBrew(streamID int64, count int64, timestamp int64,
		windowID int64, window []byte) error
	MergerBrew(streamID int64, count int64, timestamp int64,
		pendingMerges []*PendingMergeBuffer,
		heap []byte, index []byte) error
}

type InMemoryBackend struct {
	summaryMap           map[string][]byte
	landmarkMap          map[string][]byte
	heapMap              map[int64][]byte
	mergerIndexMap       map[int64][]byte
	countAndTimeMap      map[string][]byte
	summaryMapMutex      sync.Mutex
	landmarkMapMutex     sync.Mutex
	countAndTimeMapMutex sync.Mutex
}

func NewInMemoryBackend() *InMemoryBackend {
	return &InMemoryBackend{
		summaryMap:      make(map[string][]byte),
		landmarkMap:     make(map[string][]byte),
		heapMap:         make(map[int64][]byte),
		mergerIndexMap:  make(map[int64][]byte),
		countAndTimeMap: make(map[string][]byte),
	}
}

func (backend *InMemoryBackend) Get(streamID, windowID int64) ([]byte, error) {
	backend.summaryMapMutex.Lock()
	defer backend.summaryMapMutex.Unlock()
	summaryWindow, exists := backend.summaryMap[string(GetKey(false, streamID, windowID))]
	if !exists {
		return nil, errors.New("summary window not found")
	}
	return summaryWindow, nil
}

func (backend *InMemoryBackend) Put(streamID, windowID int64, buf []byte) error {
	backend.summaryMapMutex.Lock()
	defer backend.summaryMapMutex.Unlock()
	backend.summaryMap[string(GetKey(false, streamID, windowID))] = buf
	return nil
}

func (backend *InMemoryBackend) Delete(streamID, windowID int64) error {
	backend.summaryMapMutex.Lock()
	defer backend.summaryMapMutex.Unlock()
	delete(backend.summaryMap, string(GetKey(false, streamID, windowID)))
	return nil
}

func (backend *InMemoryBackend) Merge(
	streamID int64,
	windowID int64,
	buf []byte,
	deletedIDs []int64) error {
	backend.summaryMapMutex.Lock()
	defer backend.summaryMapMutex.Unlock()

	backend.summaryMap[string(GetKey(false, streamID, windowID))] = buf
	for _, ID := range deletedIDs {
		delete(backend.summaryMap, string(GetKey(false, streamID, ID)))
	}
	return nil
}

func (backend *InMemoryBackend) GetLandmark(streamID, windowID int64) ([]byte, error) {
	backend.landmarkMapMutex.Lock()
	defer backend.landmarkMapMutex.Unlock()
	landmarkWindow, exists := backend.landmarkMap[string(GetKey(true, streamID, windowID))]
	if !exists {
		return nil, errors.New("landmark window not found")
	}
	return landmarkWindow, nil
}

func (backend *InMemoryBackend) PutLandmark(streamID, windowID int64, buf []byte) error {
	backend.landmarkMapMutex.Lock()
	defer backend.landmarkMapMutex.Unlock()
	backend.landmarkMap[string(GetKey(true, streamID, windowID))] = buf
	return nil
}

func (backend *InMemoryBackend) DeleteLandmark(streamID, windowID int64) error {
	backend.landmarkMapMutex.Lock()
	defer backend.landmarkMapMutex.Unlock()
	delete(backend.landmarkMap, string(GetKey(true, streamID, windowID)))
	return nil
}

func (backend *InMemoryBackend) Close() error {
	backend.summaryMap = nil
	backend.landmarkMap = nil
	return nil
}

func (backend *InMemoryBackend) IterateIndex(streamID int64, lambda func(int64) error, landmark bool) error {
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
		err := lambda(GetWindowIDFromKey(buf))
		if err != nil {
			return err
		}
	}
	return nil
}

func (backend *InMemoryBackend) GetHeap(streamID int64) ([]byte, error) {
	heap, exists := backend.heapMap[streamID]
	if !exists {
		return nil, errors.New("heap not found")
	}
	return heap, nil
}

func (backend *InMemoryBackend) PutHeap(streamID int64, heap []byte) error {
	backend.heapMap[streamID] = heap
	return nil
}

func (backend *InMemoryBackend) GetMergerIndex(streamID int64) ([]byte, error) {
	index, exists := backend.mergerIndexMap[streamID]
	if !exists {
		return nil, errors.New("merger index not found")
	}
	return index, nil
}

func (backend *InMemoryBackend) PutMergerIndex(streamID int64, index []byte) error {
	backend.mergerIndexMap[streamID] = index
	return nil
}

func (backend *InMemoryBackend) PutCountAndTime(
	streamID int64,
	compType CompType,
	count int64,
	timestamp int64) error {
	backend.countAndTimeMapMutex.Lock()
	defer backend.countAndTimeMapMutex.Unlock()
	key := strconv.Itoa(int(streamID)) + strconv.Itoa(int(compType))
	backend.countAndTimeMap[key] = TwoInt64ToByte128(count, timestamp)
	return nil
}

func (backend *InMemoryBackend) GetCountAndTime(
	streamID int64,
	compType CompType) (int64, int64, error) {
	backend.countAndTimeMapMutex.Lock()
	defer backend.countAndTimeMapMutex.Unlock()
	key := strconv.Itoa(int(streamID)) + strconv.Itoa(int(compType))
	buf, exists := backend.countAndTimeMap[key]
	if !exists {
		return -1, -1, errors.New("count and timestamp not found")
	}
	count, timestamp := Byte128ToTwoInt64(buf)
	return count, timestamp, nil
}

func (backend *InMemoryBackend) WriterBrew(
	streamID int64, count int64, timestamp int64,
	windowID int64, window []byte) error {
	err := backend.PutCountAndTime(streamID, Writer, count, timestamp)
	if err != nil {
		return err
	}
	return backend.Put(streamID, windowID, window)
}

func (backend *InMemoryBackend) MergerBrew(
	streamID int64, count int64, timestamp int64,
	pendingMerges []*PendingMergeBuffer,
	heap []byte, index []byte) error {

	err := backend.PutCountAndTime(streamID, Merger, count, timestamp)
	if err != nil {
		return err
	}
	for _, pm := range pendingMerges {
		err = backend.Merge(streamID, pm.Id, pm.MergedWindow, pm.DeletedIDs)
	}
	if err != nil {
		return err
	}
	err = backend.PutHeap(streamID, heap)
	if err != nil {
		return err
	}
	return backend.PutMergerIndex(streamID, index)
}
