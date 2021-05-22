package storage

import "errors"

type MetadataStore interface {
	PutDB([]byte) error
	GetDB() ([]byte, error)

	PutStream(int64, []byte) error
	GetStream(int64) ([]byte, error)
}

type SimpleMetadataStore struct {
	db []byte
	streams map[int64][]byte
}

func NewSimpleMetadataStore() *SimpleMetadataStore {
	return &SimpleMetadataStore{
		db:      nil,
		streams: make(map[int64][]byte, 0),
	}
}

func (smm *SimpleMetadataStore) PutDB(db []byte) error {
	smm.db = db
	return nil
}

func (smm *SimpleMetadataStore) GetDB() ([]byte, error) {
	if smm.db == nil {
		return nil, errors.New("DB not found")
	}
	return smm.db, nil
}

func (smm *SimpleMetadataStore) PutStream(id int64, buf []byte) error {
	smm.streams[id] = buf
	return nil
}

func (smm *SimpleMetadataStore) GetStream(id int64) ([]byte, error) {
	buf, ok := smm.streams[id]
	if !ok {
		return nil, errors.New("stream not found")
	}
	return buf, nil
}