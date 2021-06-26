package core

import (
	"capnproto.org/go/capnp/v3"
	"context"
	"errors"
	"path"
	"strconv"
	"summarydb/protos"
	"summarydb/storage"
	"summarydb/window"
)

type Stream struct {
	dirName        string
	streamId       int64
	pipeline       *Pipeline
	manager        *StreamWindowManager
	backendSet     bool
	running        bool
	landmarkWindow *LandmarkWindow
}

func newWAL(dirName string, id int64) (*storage.Log, error) {
	if dirName == "" {
		return nil, nil
	}
	walPath := path.Join(dirName, "wal-"+strconv.Itoa(int(id)))
	walOpts := storage.DefaultOptions
	walOpts.NoSync = true
	walOpts.NoCopy = true
	return storage.OpenLog(walPath, walOpts)
}

func NewStreamWithId(
	dirName string,
	id int64,
	operatorNames []string,
	windowing window.Windowing) (*Stream, error) {
	manager := NewStreamWindowManager(id, operatorNames)
	wal, err := newWAL(dirName, id)
	if err != nil {
		return nil, err
	}
	pipeline := NewPipeline(windowing).SetWAL(wal)

	return &Stream{
		streamId:       id,
		pipeline:       pipeline,
		manager:        manager,
		backendSet:     false,
		running:        false,
		landmarkWindow: nil,
	}, nil
}

func (stream *Stream) SetConfig(config *StoreConfig) *Stream {
	stream.pipeline.SetBufferSize(config.EachBufferSize)
	stream.pipeline.SetWindowsPerMerge(config.WindowsPerMerge)
	return stream
}

func (stream *Stream) SetBackend(backend storage.Backend, cacheEnabled bool) *Stream {
	stream.manager.SetBackingStore(NewBackingStore(backend, cacheEnabled))
	stream.pipeline.SetWindowManager(stream.manager)
	stream.backendSet = true
	return stream
}

func (stream *Stream) Run(ctx context.Context) error {
	if !stream.backendSet {
		return errors.New("backend not set")
	}
	stream.running = true
	stream.pipeline.Run(ctx)
	return nil
}

func (stream *Stream) PrimeUp() error {
	if !stream.backendSet {
		return errors.New("backend not set")
	}
	err := stream.manager.PrimeUp()
	if err != nil {
		return err
	}
	err = stream.pipeline.PrimeUp()
	if err != nil {
		return err
	}
	err = stream.pipeline.Restore()
	if err != nil {
		return err
	}
	return nil
}

func (stream *Stream) Append(timestamp int64, value float64) error {
	if !stream.backendSet {
		panic("backend not set")
	}
	if !stream.running {
		panic("stream is not running")
	}
	var err error
	if stream.landmarkWindow != nil {
		stream.landmarkWindow.Insert(timestamp, value)
		err = nil
	} else {
		err = stream.pipeline.Append(timestamp, value)
	}
	return err
}

func (stream *Stream) StartLandmark(timestamp int64) error {
	if stream.landmarkWindow != nil {
		return errors.New("already appending as landmarks")
	}
	stream.landmarkWindow = NewLandmarkWindow(timestamp)
	return nil
}

func (stream *Stream) EndLandmark(timestamp int64) error {
	if stream.landmarkWindow == nil {
		return errors.New("no running landmark")
	}
	stream.landmarkWindow.Close(timestamp)
	err := stream.manager.PutLandmarkWindow(stream.landmarkWindow)
	stream.landmarkWindow = nil
	return err
}

func (stream *Stream) Flush() error {
	return stream.pipeline.Flush(false)
}

func (stream *Stream) Close() error {
	return stream.pipeline.Flush(true)
}

func (stream *Stream) Query(
	op string,
	startTime int64,
	endTime int64,
	params *QueryParams) (*AggResult, error) {
	if !stream.backendSet {
		panic("backend not set")
	}

	if stream.running {
		// sync writes
		err := stream.Flush()
		if err != nil {
			return nil, err
		}
	}

	summaryWindows, err := stream.pipeline.streamWindowManager.
		GetSummaryWindowInRange(startTime, endTime)
	if err != nil {
		return nil, err
	}
	landmarkWindows, err := stream.pipeline.streamWindowManager.
		GetLandmarkWindowInRange(startTime, endTime)
	if err != nil {
		return nil, err
	}

	opCompute := stream.manager.operators.GetOp(op)

	return opCompute.Query(
		summaryWindows,
		landmarkWindows,
		startTime,
		endTime,
		params), nil
}

func (stream *Stream) Serialize() ([]byte, error) {
	msg, seg, err := capnp.NewMessage(capnp.SingleSegment(nil))
	if err != nil {
		return nil, err
	}
	streamProto, err := protos.NewRootStream(seg)
	if err != nil {
		return nil, err
	}

	// ID
	streamProto.SetId(stream.streamId)

	// Operators
	opSet := stream.manager.operators
	opProtoList, err := streamProto.NewOperators(int32(len(opSet.ops)))
	if err != nil {
		return nil, err
	}
	it := 0
	for _, v := range opSet.ops {
		opProtoList.Set(it, v.GetOpType())
		it += 1
	}

	// Windowing
	seq := stream.pipeline.windowing.GetSeq()
	windowProto := streamProto.Window()
	err = seq.Serialize(&windowProto)
	if err != nil {
		return nil, err
	}

	// Marshal
	buf, err := msg.Marshal()
	if err != nil {
		panic(err)
		return nil, err
	}
	return buf, nil
}

func DeserializeStream(dirName string, buf []byte) (*Stream, error) {
	msg, err := capnp.Unmarshal(buf)
	if err != nil {
		return nil, err
	}

	streamProto, err := protos.ReadRootStream(msg)
	if err != nil {
		return nil, err
	}

	id := streamProto.Id()
	ops, err := streamProto.Operators()
	if err != nil {
		return nil, err
	}
	opNames := OpProtosToOpNames(ops)

	windowProto := streamProto.Window()
	seq, err := window.DeserializeLengthsSequence(&windowProto)
	if err != nil {
		return nil, err
	}
	windowing := window.NewGenericWindowing(seq)
	return NewStreamWithId(dirName, id, opNames, windowing)
}
