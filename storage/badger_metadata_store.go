package storage

import (
	"encoding/binary"
	"github.com/dgraph-io/badger/v2"
)

const DbKey = "DBKEY"

type BadgerMetadataStore struct {
	db *badger.DB
}

func NewBadgerMetadataStore(db *badger.DB) *BadgerMetadataStore {
	return &BadgerMetadataStore{db: db}
}

func (bms *BadgerMetadataStore) PutDB(buf []byte) error {
	return bms.db.Update(func (txn *badger.Txn) error {
		return txn.Set([]byte(DbKey), buf)
	})
}

func (bms *BadgerMetadataStore) GetDB() ([]byte, error) {
	var dbBytes []byte
	err := bms.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(DbKey))
		if err != nil {
			return err
		}
		dbBytes, err = item.ValueCopy(nil)
		return err
	})
	return dbBytes, err
}

func GetByteKey(streamId int64) []byte {
	key := make([]byte, 8)
	binary.LittleEndian.PutUint64(key, uint64(streamId))
	return key
}

func (bms *BadgerMetadataStore) PutStream(streamId int64, buf []byte) error {
	return bms.db.Update(func(txn *badger.Txn) error {
		return txn.Set(GetByteKey(streamId), buf)
	})
}

func (bms *BadgerMetadataStore) GetStream(streamId int64) ([]byte, error) {
	var streamBytes []byte
	err := bms.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(GetByteKey(streamId))
		if err != nil {
			return err
		}
		streamBytes, err = item.ValueCopy(nil)
		return err
	})
	return streamBytes, err
}