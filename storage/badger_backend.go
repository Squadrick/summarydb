package storage

import (
	"github.com/dgraph-io/badger/v2"
	"log"
)

type BadgerBackendConfig struct {
	Path     string
	InMemory bool
}

func TestBadgerBackendConfig() *BadgerBackendConfig {
	return &BadgerBackendConfig{
		Path:     "",
		InMemory: true,
	}
}

type BadgerBackend struct {
	db *badger.DB
}

func NewBadgerBacked(config *BadgerBackendConfig) *BadgerBackend {
	option := badger.DefaultOptions(config.Path).WithInMemory(config.InMemory)
	db, err := badger.Open(option)
	if err != nil {
		log.Fatal(err)
	}
	return &BadgerBackend{db: db}
}

func (backend *BadgerBackend) Close() {
	err := backend.db.Close()
	if err != nil {
		log.Fatal(err)
	}
}

func (backend *BadgerBackend) txnGet(key []byte) []byte {
	var windowBytes []byte
	err := backend.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}
		windowBytes, err = item.ValueCopy(nil)

		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}
	return windowBytes
}

func (backend *BadgerBackend) txnPut(key, buf []byte) {
	err := backend.db.Update(func(txn *badger.Txn) error {
		err := txn.Set(key, buf)
		return err
	})

	if err != nil {
		log.Fatal(err)
	}
}

func (backend *BadgerBackend) txnDelete(key []byte) {
	err := backend.db.Update(func(txn *badger.Txn) error {
		err := txn.Delete(key)
		return err
	})

	if err != nil {
		log.Fatal(err)
	}
}

func (backend *BadgerBackend) Get(streamID, windowID int64) []byte {
	key := GetKey(false, streamID, windowID)
	return backend.txnGet(key)
}

func (backend *BadgerBackend) Put(streamID, windowID int64, buf []byte) {
	key := GetKey(false, streamID, windowID)
	backend.txnPut(key, buf)
}

func (backend *BadgerBackend) Delete(streamID, windowID int64) {
	key := GetKey(false, streamID, windowID)
	backend.txnDelete(key)
}

func (backend *BadgerBackend) GetLandmark(streamID, windowID int64) []byte {
	key := GetKey(true, streamID, windowID)
	return backend.txnGet(key)
}

func (backend *BadgerBackend) PutLandmark(streamID, windowID int64, buf []byte) {
	key := GetKey(true, streamID, windowID)
	backend.txnPut(key, buf)
}

func (backend *BadgerBackend) DeleteLandmark(streamID, windowID int64) {
	key := GetKey(true, streamID, windowID)
	backend.txnDelete(key)
}

func (backend *BadgerBackend) IterateIndex(streamID int64, lambda func(int64), landmark bool) {
	return
}
