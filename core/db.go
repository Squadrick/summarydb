package core

import (
	"capnproto.org/go/capnp/v3"
	"errors"
	"github.com/dgraph-io/badger/v2"
	"summarydb/protos"
	"summarydb/storage"
	"summarydb/window"
	"sync"
	"sync/atomic"
)

var gStreamIdCounter int64 = 0

type DB struct {
	backend storage.Backend
	mds     storage.MetadataStore
	streams map[int64]*Stream
	mu      sync.Mutex
}

func New(path string) (*DB, error) {
	badgerOptions := badger.DefaultOptions(path).WithTruncate(true)
	badgerDb, err := badger.Open(badgerOptions)
	badgerBackend := storage.NewBadgerBacked(badgerDb)
	if err != nil {
		return nil, err
	}

	db := &DB{
		backend: badgerBackend,
		mds:     storage.NewBadgerMetadataStore(badgerDb),
		streams: make(map[int64]*Stream),
		mu:      sync.Mutex{},
	}

	return db, nil
}

func Open(path string) (*DB, error) {
	db, err := New(path)
	if err != nil {
		return nil, err
	}
	err = db.ReadDB()
	if err != nil {
		return nil, err
	}
	return db, nil
}

func (db *DB) NewStream(operatorNames []string, seq window.LengthsSequence) (*Stream, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	defer atomic.AddInt64(&gStreamIdCounter, 1)
	streamId := gStreamIdCounter
	windowing := window.NewGenericWindowing(seq)
	stream := NewStreamWithId(streamId, operatorNames, windowing).
		SetBackend(db.backend, true)
	db.streams[streamId] = stream

	err := db.WriteDBAndStream(stream)
	if err != nil {
		return nil, err
	}

	return stream, nil
}

func (db *DB) GetStream(streamId int64) (*Stream, error) {
	stream, ok := db.streams[streamId]
	if !ok {
		return nil, errors.New("stream not found")
	}
	return stream, nil
}

func (db *DB) Close() error {
	// TODO: Detect when the stream has already been stopped.
	//for _, stream := range db.streams {
	//	stream.Close()
	//}
	return db.backend.Close()
}

func (db *DB) WriteDBAndStream(stream *Stream) error {
	dbBuf, err := db.Serialize()
	if err != nil {
		return err
	}
	streamBuf, err := stream.Serialize()
	if err != nil {
		return err
	}
	return db.mds.PutDBAndStream(dbBuf, stream.streamId, streamBuf)
}

func (db *DB) ReadDB() error {
	buf, err := db.mds.GetDB()
	if err != nil {
		// TODO: for now consider that this is a new DB being created.
		return nil
	}
	msg, err := capnp.Unmarshal(buf)
	if err != nil {
		return err
	}
	dbProto, err := protos.ReadRootDB(msg)
	if err != nil {
		return err
	}
	streamIds, err := dbProto.StreamIds()
	if err != nil {
		return err
	}

	for i := 0; i < streamIds.Len(); i++ {
		streamId := streamIds.At(i)
		streamBuf, err := db.mds.GetStream(streamId)
		if err != nil {
			return err
		}
		stream, err := DeserializeStream(streamBuf)
		if err != nil {
			return err
		}
		db.streams[streamId] = stream
		stream.SetBackend(db.backend, true)
		err = stream.PrimeUp()
		if err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) Serialize() ([]byte, error) {
	msg, seg, err := capnp.NewMessage(capnp.SingleSegment(nil))
	if err != nil {
		return nil, err
	}
	dbProto, err := protos.NewRootDB(seg)
	if err != nil {
		return nil, err
	}

	streamIdsProto, err := dbProto.NewStreamIds(int32(len(db.streams)))
	if err != nil {
		return nil, err
	}
	it := 0
	for id := range db.streams {
		streamIdsProto.Set(it, id)
		it += 1
	}

	buf, err := msg.Marshal()
	if err != nil {
		return nil, err
	}
	return buf, nil
}
