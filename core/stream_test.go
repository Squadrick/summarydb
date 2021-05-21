package core

import (
	"github.com/stretchr/testify/assert"
	"summarydb/window"
	"testing"
)

func TestStream_Serialize_Deserialize(t *testing.T) {
	exp := window.NewExponentialLengthsSequence(2)
	windowing := window.NewGenericWindowing(exp)
	stream := NewStream([]string{"count", "max", "sum"},
		windowing)
	bytes := stream.Serialize()

	newStream := DeserializeStream(bytes)

	assert.Equal(t, stream.streamId, newStream.streamId)
	//assert.True(t, stream.pipeline.windowing.GetSeq().Equals(
	//	newStream.pipeline.windowing.GetSeq()))
	assert.True(t, stream.manager.operators.Equals(
		newStream.manager.operators))
}
