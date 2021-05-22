package core

import (
	"summarydb/protos"
	"summarydb/storage"
	"summarydb/window"
	"sync/atomic"
	capnp "zombiezen.com/go/capnproto2"
)

var gStreamIdCounter int64 = 0

type DB struct {
	mds storage.MetadataStore
	streams map[int64]*Stream
}

func NewDB() *DB {
	return &DB{
		mds:     storage.NewSimpleMetadataStore(),
		streams: make(map[int64]*Stream),
	}
}

func (db *DB) NewStream(operatorNames []string, seq window.LengthsSequence) *Stream {
	defer atomic.AddInt64(&gStreamIdCounter, 1)
	streamId := gStreamIdCounter
	windowing := window.NewGenericWindowing(seq)
	stream := NewStreamWithId(streamId, operatorNames, windowing)
	db.streams[streamId] = stream
	db.WriteDB()
	return stream
}

func (db *DB) Close() error {
	for _, stream := range db.streams {
		stream.Shutdown()
		err := db.WriteStream(stream)
		if err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) WriteStream(stream *Stream) error {
	buf := stream.Serialize()
	return db.mds.PutStream(stream.streamId, buf)
}

func (db *DB) WriteDB() {
	buf := db.Serialize()
	err := db.mds.PutDB(buf)
	if err != nil {
		panic(err)
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