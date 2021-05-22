package core

import (
	"context"
	"summarydb/protos"
	"summarydb/storage"
	"summarydb/window"
	capnp "zombiezen.com/go/capnproto2"
)

type Stream struct {
	streamId int64
	pipeline *Pipeline
	manager  *StreamWindowManager
}

func NewStreamWithId(
	id int64,
	operatorNames []string,
	windowing window.Windowing) *Stream {
	manager := NewStreamWindowManager(id, operatorNames)
	pipeline := NewPipeline(windowing)
	return &Stream{
		streamId: id,
		pipeline: pipeline,
		manager:  manager,
	}
}

func (stream *Stream) SetConfig(config *StoreConfig) *Stream {
	stream.pipeline.SetBufferSize(config.EachBufferSize)
	stream.pipeline.SetWindowsPerMerge(config.WindowsPerMerge)
	return stream
}

func (stream *Stream) SetBackingStore(store *BackingStore) *Stream {
	stream.manager.SetBackingStore(store)
	return stream
}

func (stream *Stream) Run(ctx context.Context) {
	stream.pipeline.Run(ctx)
}

func (stream *Stream) SetBackend(backend storage.Backend, cacheEnabled bool) {
	stream.manager.SetBackingStore(NewBackingStore(backend, cacheEnabled))
	stream.pipeline.SetWindowManager(stream.manager)
}

func (stream *Stream) Append(timestamp int64, value float64) {
	stream.pipeline.Append(timestamp, value)
}

func (stream *Stream) Flush() {
	stream.pipeline.Flush(false)
}

func (stream *Stream) Shutdown() {
	stream.pipeline.Flush(true)
}

func (stream *Stream) Query(
	op string,
	startTime int64,
	endTime int64,
	params *QueryParams) *AggResult {

	stream.pipeline.Flush(false) // sync writes
	summaryWindows := stream.pipeline.streamWindowManager.
		GetSummaryWindowInRange(startTime, endTime)
	landmarkWindows := stream.pipeline.streamWindowManager.
		GetLandmarkWindowInRange(startTime, endTime)

	opCompute := stream.manager.operators.GetOp(op)

	return opCompute.Query(
		summaryWindows,
		landmarkWindows,
		startTime,
		endTime,
		params)
}

func (stream *Stream) Serialize() []byte {
	msg, seg, err := capnp.NewMessage(capnp.SingleSegment(nil))
	streamProto, err := protos.NewRootStream(seg)
	if err != nil {
		panic(err)
	}

	// ID
	streamProto.SetId(stream.streamId)

	// Operators
	opSet := stream.manager.operators
	opProtoList, err := streamProto.NewOperators(int32(len(opSet.ops)))
	if err != nil {
		panic(err)
	}
	it := 0
	for _, v := range opSet.ops {
		opProtoList.Set(it, v.GetOpType())
		it += 1
	}

	// Windowing
	seq := stream.pipeline.windowing.GetSeq()
	windowProto := streamProto.Window()
	seq.Serialize(&windowProto)

	// Marshal
	buf, err := msg.Marshal()
	if err != nil {
		panic(err)
	}
	return buf
}

func DeserializeStream(buf []byte) *Stream {
	msg, err := capnp.Unmarshal(buf)
	if err != nil {
		panic(err)
	}

	streamProto, err := protos.ReadRootStream(msg)
	if err != nil {
		panic(err)
	}

	id := streamProto.Id()
	ops, err := streamProto.Operators()
	if err != nil {
		panic(err)
	}
	opNames := OpProtosToOpNames(ops)

	windowProto := streamProto.Window()
	seq := window.DeserializeLengthsSequence(&windowProto)
	windowing := window.NewGenericWindowing(seq)
	return NewStreamWithId(id, opNames, windowing)
}
