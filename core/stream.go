package core

import (
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
