package storage

import "errors"

type MetadataStore interface {
	PutDBAndStream([]byte, int64, []byte) error
	GetDB() ([]byte, error)
	GetStream(int64) ([]byte, error)
}

type SimpleMetadataStore struct {
	db      []byte
	streams map[int64][]byte
}

func NewSimpleMetadataStore() *SimpleMetadataStore {
	return &SimpleMetadataStore{
		db:      nil,
		streams: make(map[int64][]byte, 0),
	}
}

func (smm *SimpleMetadataStore) PutDBAndStream(
	dbBuf []byte, id int64, streamBuf []byte) error {
	smm.db = dbBuf
	smm.streams[id] = streamBuf
	return nil
}

func (smm *SimpleMetadataStore) GetDB() ([]byte, error) {
	if smm.db == nil {
		return nil, errors.New("DB not found")
	}
	return smm.db, nil
}

func (smm *SimpleMetadataStore) GetStream(id int64) ([]byte, error) {
	buf, ok := smm.streams[id]
	if !ok {
		return nil, errors.New("stream not found")
	}
	return buf, nil
}
