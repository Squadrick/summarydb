package core

import (
	"context"
	"summarydb/storage"
	"summarydb/window"
)

type Stream struct {
	streamId int64
	pipeline *Pipeline
	manager  *StreamWindowManager
}

func NewStream(
	operatorNames []string,
	windowing window.Windowing,
	config *StoreConfig) *Stream {
	manager := NewStreamWindowManager(0, operatorNames)

	pipeline := NewPipeline(windowing)
	pipeline.SetBufferSize(config.TotalBufferSize, config.NumBuffer)
	pipeline.SetWindowsPerBatch(config.WindowsPerBatch)

	return &Stream{
		streamId: 0,
		pipeline: pipeline,
		manager:  manager,
	}
}

func (stream *Stream) SetBackend(backend storage.Backend) {
	stream.manager.SetBackingStore(NewBackingStore(backend))
	stream.pipeline.SetWindowManager(stream.manager)
}

func (stream *Stream) Append(timestamp int64, value float64) {
	stream.pipeline.Append(timestamp, value)
}

func (stream *Stream) Flush() {
	stream.pipeline.Flush(false, true)
}

func (stream *Stream) Shutdown() {
	stream.pipeline.Flush(true, true)
}

func (stream *Stream) StartPipeline(ctx context.Context) {
	stream.pipeline.Run(ctx)
}

func (stream *Stream) Query(
	op string,
	startTime int64,
	endTime int64,
	params *QueryParams) *AggResult {

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
