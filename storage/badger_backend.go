package storage

import (
	"encoding/binary"
	"github.com/dgraph-io/badger/v2"
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

func (backend *BadgerBackend) Close() error {
	return backend.db.Close()
}

func (backend *BadgerBackend) txnGet(key []byte) ([]byte, error) {
	var windowBytes []byte
	err := backend.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}
		windowBytes, err = item.ValueCopy(nil)
		return err
	})
	return windowBytes, err
}

func (backend *BadgerBackend) txnPut(key, buf []byte) error {
	err := backend.db.Update(func(txn *badger.Txn) error {
		err := txn.Set(key, buf)
		return err
	})
	return err
}

func (backend *BadgerBackend) txnDelete(key []byte) error {
	err := backend.db.Update(func(txn *badger.Txn) error {
		err := txn.Delete(key)
		return err
	})
	return err
}

func (backend *BadgerBackend) Get(streamID, windowID int64) ([]byte, error) {
	key := GetKey(false, streamID, windowID)
	return backend.txnGet(key)
}

func (backend *BadgerBackend) Put(streamID, windowID int64, buf []byte) error {
	key := GetKey(false, streamID, windowID)
	return backend.txnPut(key, buf)
}

func (backend *BadgerBackend) Delete(streamID, windowID int64) error {
	key := GetKey(false, streamID, windowID)
	return backend.txnDelete(key)
}

func mergeTxnFunc(txn *badger.Txn,
	wKey []byte, wBuf []byte, delKeys [][]byte) error {
	err := txn.Set(wKey, wBuf)
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
}

func (backend *BadgerBackend) Merge(
	streamID int64,
	windowID int64,
	buf []byte,
	deletedIDs []int64) error {

	key := GetKey(false, streamID, windowID)
	delKeys := make([][]byte, len(deletedIDs))

	for i, ID := range deletedIDs {
		delKeys[i] = GetKey(false, streamID, ID)
	}

	err := backend.db.Update(func(txn *badger.Txn) error {
		return mergeTxnFunc(txn, key, buf, delKeys)
	})
	return err
}

func (backend *BadgerBackend) GetLandmark(streamID, windowID int64) ([]byte, error) {
	key := GetKey(true, streamID, windowID)
	return backend.txnGet(key)
}

func (backend *BadgerBackend) PutLandmark(streamID, windowID int64, buf []byte) error {
	key := GetKey(true, streamID, windowID)
	return backend.txnPut(key, buf)
}

func (backend *BadgerBackend) DeleteLandmark(streamID, windowID int64) error {
	key := GetKey(true, streamID, windowID)
	return backend.txnDelete(key)
}

func (backend *BadgerBackend) GetHeap(streamID int64) ([]byte, error) {
	key := GetKey(false, streamID, math.MinInt64+HeapOffset)
	return backend.txnGet(key)
}

func (backend *BadgerBackend) PutHeap(streamID int64, heap []byte) error {
	key := GetKey(false, streamID, math.MinInt64+HeapOffset)
	return backend.txnPut(key, heap)
}

func (backend *BadgerBackend) GetMergerIndex(streamID int64) ([]byte, error) {
	key := GetKey(false, streamID, math.MinInt64+MergerIndexOffset)
	return backend.txnGet(key)
}

func (backend *BadgerBackend) PutMergerIndex(streamID int64, index []byte) error {
	key := GetKey(false, streamID, math.MinInt64+MergerIndexOffset)
	return backend.txnPut(key, index)
}

func (backend *BadgerBackend) PutCountAndTime(
	streamID int64,
	compType CompType,
	count int64,
	timestamp int64) error {
	key := GetKey(false, streamID,
		math.MinInt64+CompTypeOffset+int64(compType))
	buf := TwoInt64ToByte128(count, timestamp)
	return backend.txnPut(key, buf)
}

func (backend *BadgerBackend) GetCountAndTime(
	streamID int64,
	compType CompType) (int64, int64, error) {
	key := GetKey(false, streamID,
		math.MinInt64+CompTypeOffset+int64(compType))
	buf, err := backend.txnGet(key)
	if err != nil {
		return -1, -1, err
	}
	count, timestamp := Byte128ToTwoInt64(buf)
	return count, timestamp, nil
}

func GetKeyPrefix(landmark bool, streamID int64) []byte {
	buf := make([]byte, 9)
	binary.LittleEndian.PutUint64(buf[:8], uint64(streamID))
	buf[8] = BitSet(landmark)
	return buf
}

func (backend *BadgerBackend) IterateIndex(streamID int64, lambda func(int64) error, landmark bool) error {
	prefix := GetKeyPrefix(landmark, streamID)
	iterOpts := badger.IteratorOptions{Prefix: prefix}
	err := backend.db.View(func(txn *badger.Txn) error {
		iter := txn.NewIterator(iterOpts)
		defer iter.Close()

		for iter.Seek(nil); iter.Valid(); iter.Next() {
			item := iter.Item()
			windowId := GetWindowIDFromKey(item.Key())
			// MinInt64 to MinInt64 + Footer is reserved for special entries.
			if windowId > math.MinInt64+Footer {
				err := lambda(GetWindowIDFromKey(item.Key()))
				if err != nil {
					return err
				}
			}
		}
		return nil
	})
	return err
}

// --- special brews ---

func (backend *BadgerBackend) WriterBrew(
	streamID int64, count int64, timestamp int64,
	windowID int64, window []byte) error {
	wKey := GetKey(false, streamID, windowID)
	ctKey := GetKey(false, streamID,
		math.MinInt64+CompTypeOffset+int64(Writer))
	ctBuf := TwoInt64ToByte128(count, timestamp)
	return backend.db.Update(func(txn *badger.Txn) error {
		err := txn.Set(wKey, window)
		if err != nil {
			return err
		}
		return txn.Set(ctKey, ctBuf)
	})
}

func (backend *BadgerBackend) MergerBrew(
	streamID int64, count int64, timestamp int64,
	pendingMerges []*PendingMergeBuffer,
	heap []byte, index []byte) error {
	ctKey := GetKey(false, streamID,
		math.MinInt64+CompTypeOffset+int64(Merger))
	ctBuf := TwoInt64ToByte128(count, timestamp)
	mKey := GetKey(false, streamID, math.MinInt64+MergerIndexOffset)
	hKey := GetKey(false, streamID, math.MinInt64+HeapOffset)

	return backend.db.Update(func(txn *badger.Txn) error {
		err := txn.Set(ctKey, ctBuf)
		if err != nil {
			return err
		}
		err = txn.Set(mKey, index)
		if err != nil {
			return err
		}
		err = txn.Set(hKey, heap)
		if err != nil {
			return err
		}

		for _, pm := range pendingMerges {
			wKey := GetKey(false, streamID, pm.Id)
			delKeys := make([][]byte, len(pm.DeletedIDs))
			for i, ID := range pm.DeletedIDs {
				delKeys[i] = GetKey(false, streamID, ID)
			}
			err := mergeTxnFunc(txn, wKey, pm.MergedWindow, delKeys)
			if err != nil {
				return err
			}
		}
		return nil
	})
}
