package storage

import (
	"encoding/binary"
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

func GetBadgerKey(landmark bool, streamID, windowID int64) []byte {
	buf := make([]byte, 17)
	if landmark {
		buf[0] = 1
	} else {
		buf[0] = 0
	}

	binary.LittleEndian.PutUint64(buf[1:9], uint64(windowID))
	binary.LittleEndian.PutUint64(buf[9:], uint64(streamID))
	return buf
}

func (backend *BadgerBackend) Get(streamID, windowID int64) []byte {
	key := GetBadgerKey(false, streamID, windowID)
	return backend.txnGet(key)
}

func (backend *BadgerBackend) Put(streamID, windowID int64, buf []byte) {
	key := GetBadgerKey(false, streamID, windowID)
	backend.txnPut(key, buf)
}

func (backend *BadgerBackend) Delete(streamID, windowID int64) {
	key := GetBadgerKey(false, streamID, windowID)
	backend.txnDelete(key)
}

func (backend *BadgerBackend) GetLandmark(streamID, windowID int64) []byte {
	key := GetBadgerKey(true, streamID, windowID)
	return backend.txnGet(key)
}

func (backend *BadgerBackend) PutLandmark(streamID, windowID int64, buf []byte) {
	key := GetBadgerKey(true, streamID, windowID)
	backend.txnPut(key, buf)
}

func (backend *BadgerBackend) DeleteLandmark(streamID, windowID int64) {
	key := GetBadgerKey(true, streamID, windowID)
	backend.txnDelete(key)
}
