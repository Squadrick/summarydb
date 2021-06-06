package core

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestIngestBuffer_Append(t *testing.T) {
	outputChannel := make(chan *IngestBuffer, 10)
	in := NewIngester(outputChannel)
	in.setBufferCapacity(10)
	in.allocator.SetMaxBuffers(100)

	for i := 0; i < 20; i++ {
		in.Append(int64(i), float64(i))
	}
	in.Flush(false)
	close(outputChannel)
	buffers := make([]*IngestBuffer, 0)
	for buffer := range outputChannel {
		buffers = append(buffers, buffer)
	}
	assert.Equal(t, len(buffers), 3)
	assert.Equal(t, buffers[2], ConstFlushIngestBuffer())

	assert.Equal(t, buffers[0].Size, int64(10))
	assert.Equal(t, buffers[0].timestamps[0], int64(0))
	assert.Equal(t, buffers[0].timestamps[9], int64(9))

	assert.Equal(t, buffers[1].Size, int64(10))
	assert.Equal(t, buffers[1].timestamps[0], int64(10))
	assert.Equal(t, buffers[1].timestamps[9], int64(19))
}
