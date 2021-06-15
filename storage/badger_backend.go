package storage

import (
	"encoding/binary"
	"github.com/dgraph-io/badger/v2"
	"log"
	"math"
)

const (
	HeapOffset        = iota
	MergerIndexOffset = iota
	CompTypeOffset    = iota // with landmark = true
	CompTypeOffset_P  = iota
	CompTypeOffset_W  = iota
	CompTypeOffset_M  = iota
	Footer            = iota
)

func TestBadgerDB() *badger.DB {
	option := badger.DefaultOptions("").WithInMemory(true)
	db, err := badger.Open(option)
	if err != nil {
		panic(err)
	}
	return db
}

type BadgerBackend struct {
	db *badger.DB
}

func NewBadgerBacked(db *badger.DB) *BadgerBackend {
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
		return err
	})
	if err != nil {
		panic(err)
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

func (backend *BadgerBackend) Merge(
	streamID int64,
	windowID int64,
	buf []byte,
	deletedIDs []int64) {

	key := GetKey(false, streamID, windowID)
	delKeys := make([][]byte, len(deletedIDs))

	for i, ID := range deletedIDs {
		delKeys[i] = GetKey(false, streamID, ID)
	}

	err := backend.db.Update(func(txn *badger.Txn) error {
		err := txn.Set(key, buf)
		if err != nil {
			return err
		}

		for _, delKey := range delKeys {
			err := txn.Delete(delKey)
			if err != nil {
				return err
			}
		}
		return nil
	})

	if err != nil {
		log.Fatal(err)
	}
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

func (backend *BadgerBackend) GetHeap(streamID int64) []byte {
	key := GetKey(false, streamID, math.MinInt64+HeapOffset)
	return backend.txnGet(key)
}

func (backend *BadgerBackend) PutHeap(streamID int64, heap []byte) {
	key := GetKey(false, streamID, math.MinInt64+HeapOffset)
	backend.txnPut(key, heap)
}

func (backend *BadgerBackend) GetMergerIndex(streamID int64) []byte {
	key := GetKey(false, streamID, math.MinInt64+MergerIndexOffset)
	return backend.txnGet(key)
}

func (backend *BadgerBackend) PutMergerIndex(streamID int64, index []byte) {
	key := GetKey(false, streamID, math.MinInt64+MergerIndexOffset)
	backend.txnPut(key, index)
}

func (backend *BadgerBackend) PutCountAndTime(
	streamID int64,
	compType CompType,
	count int64,
	timestamp int64) {
	key := GetKey(true, streamID,
		math.MinInt64+CompTypeOffset+int64(compType))
	buf := TwoInt64ToByte128(count, timestamp)
	backend.txnPut(key, buf)
}

func (backend *BadgerBackend) GetCountAndTime(
	streamID int64,
	compType CompType) (int64, int64) {
	key := GetKey(true, streamID,
		math.MinInt64+CompTypeOffset+int64(compType))
	buf := backend.txnGet(key)
	return Byte128ToTwoInt64(buf)
}

func GetKeyPrefix(landmark bool, streamID int64) []byte {
	buf := make([]byte, 9)
	binary.LittleEndian.PutUint64(buf[:8], uint64(streamID))
	buf[8] = BitSet(landmark)
	return buf
}

func (backend *BadgerBackend) IterateIndex(streamID int64, lambda func(int64), landmark bool) {
	prefix := GetKeyPrefix(landmark, streamID)
	iterOpts := badger.IteratorOptions{Prefix: prefix}
	_ = backend.db.View(func(txn *badger.Txn) error {
		iter := txn.NewIterator(iterOpts)
		defer iter.Close()

		for iter.Seek(nil); iter.Valid(); iter.Next() {
			item := iter.Item()
			windowId := GetWindowIDFromKey(item.Key())
			// MinInt64 to MinInt64 + Footer is reserved for special entries.
			if windowId > math.MinInt64+Footer {
				lambda(GetWindowIDFromKey(item.Key()))
			}
		}
		return nil
	})
}
