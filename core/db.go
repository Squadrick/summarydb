package core

import (
	"github.com/dgraph-io/badger/v2"
	"summarydb/protos"
	"summarydb/storage"
	"summarydb/window"
	"sync/atomic"
	capnp "zombiezen.com/go/capnproto2"
)

var gStreamIdCounter int64 = 0

type DB struct {
	backend storage.Backend
	mds storage.MetadataStore
	streams map[int64]*Stream
}

func Open(path string) *DB {
	badgerOptions := badger.DefaultOptions(path)
	badgerDb, err := badger.Open(badgerOptions)
	badgerBackend := storage.NewBadgerBacked(badgerDb)
	if err != nil {
		panic(err)
	}

	db := &DB{
		backend: badgerBackend,
		mds:     storage.NewBadgerMetadataStore(badgerDb),
		streams: make(map[int64]*Stream),
	}

	db.ReadDB()
	return db
}

func (db *DB) NewStream(operatorNames []string, seq window.LengthsSequence) *Stream {
	defer atomic.AddInt64(&gStreamIdCounter, 1)
	streamId := gStreamIdCounter
	windowing := window.NewGenericWindowing(seq)
	stream := NewStreamWithId(streamId, operatorNames, windowing).
		SetBackend(db.backend, true)
	db.streams[streamId] = stream

	// TODO: Make this a single transaction.
	db.WriteDB()
	db.WriteStream(stream)

	return stream
}

func (db *DB) GetStream(streamId int64) *Stream {
	return db.streams[streamId]
}

func (db *DB) Close() error {
	for _, stream := range db.streams {
		stream.Close()
	}
	db.backend.Close()
	return nil
}

func (db *DB) WriteStream(stream *Stream) {
	buf := stream.Serialize()
	err := db.mds.PutStream(stream.streamId, buf)
	if err != nil {
		panic(err)
	}
}

func (db *DB) WriteDB() {
	buf := db.Serialize()
	err := db.mds.PutDB(buf)
	if err != nil {
		panic(err)
	}
}

func (db *DB) ReadDB() {
	buf, err := db.mds.GetDB()
	if err != nil {
		// for now consider that this is a new
		// DB being created.
		return
	}
	msg, err := capnp.Unmarshal(buf)
	if err != nil {
		panic(err)
	}
	dbProto, err := protos.ReadRootDB(msg)
	if err != nil {
		panic(err)
	}
	streamIds, err := dbProto.StreamIds()
	if err != nil {
		panic(err)
	}

	for i := 0; i < streamIds.Len(); i++ {
		streamId := streamIds.At(i)
		streamBuf, err := db.mds.GetStream(streamId)
		if err != nil {
			panic(err)
		}
		stream := DeserializeStream(streamBuf)
		db.streams[streamId] = stream
	}
}

func (db *DB) Serialize() []byte {
	msg, seg, err := capnp.NewMessage(capnp.SingleSegment(nil))
	dbProto, err := protos.NewRootDB(seg)
	if err != nil {
		panic(err)
	}

	streamIdsProto, err := dbProto.NewStreamIds(int32(len(db.streams)))
	if err != nil {
		panic(err)
	}
	it := 0
	for id := range db.streams {
		streamIdsProto.Set(it, id)
		it += 1
	}

	buf, err := msg.Marshal()
	if err != nil {
		panic(err)
	}
	return buf
}