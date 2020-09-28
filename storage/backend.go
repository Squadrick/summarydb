package storage

import "strconv"

func GetKey(a, b int64) string {
	return strconv.Itoa(int(a)) + "-" + strconv.Itoa(int(b))
}

type Backend interface {
	Get(int64, int64) []byte
	Put(int64, int64, []byte)
	Delete(int64, int64)

	GetLandmark(int64, int64) []byte
	PutLandmark(int64, int64, []byte)
	DeleteLandmark(int64, int64)
}

type InMemoryBackend struct {
	summaryMap  map[string][]byte
	landmarkMap map[string][]byte
}

func NewInMemoryBackend() *InMemoryBackend {
	return &InMemoryBackend{
		summaryMap:  make(map[string][]byte),
		landmarkMap: make(map[string][]byte),
	}
}

func (backend *InMemoryBackend) Get(streamID, windowID int64) []byte {
	return backend.summaryMap[GetKey(streamID, windowID)]
}

func (backend *InMemoryBackend) Put(streamID, windowID int64, buf []byte) {
	backend.summaryMap[GetKey(streamID, windowID)] = buf
}

func (backend *InMemoryBackend) Delete(streamID, windowID int64) {
	delete(backend.summaryMap, GetKey(streamID, windowID))
}

func (backend *InMemoryBackend) GetLandmark(streamID, windowID int64) []byte {
	return backend.landmarkMap[GetKey(streamID, windowID)]
}

func (backend *InMemoryBackend) PutLandmark(streamID, windowID int64, buf []byte) {
	backend.landmarkMap[GetKey(streamID, windowID)] = buf
}

func (backend *InMemoryBackend) DeleteLandmark(streamID, windowID int64) {
	delete(backend.landmarkMap, GetKey(streamID, windowID))
}
