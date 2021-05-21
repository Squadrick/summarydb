package core

import (
	"github.com/stretchr/testify/assert"
	"summarydb/window"
	"testing"
)

func testStreamSerializeDeserialize(t *testing.T, seq window.LengthsSequence) {
	windowing := window.NewGenericWindowing(seq)
	stream := NewStream([]string{"count", "max", "sum"},
		windowing)
	bytes := stream.Serialize()

	newStream := DeserializeStream(bytes)

	assert.Equal(t, stream.streamId, newStream.streamId)
	assert.True(t, stream.pipeline.windowing.GetSeq().Equals(
		newStream.pipeline.windowing.GetSeq()))
	assert.True(t, stream.manager.operators.Equals(
		newStream.manager.operators))
}

func TestStream_Serialize_Deserialize_Exp(t *testing.T) {
	exp := window.NewExponentialLengthsSequence(2)
	testStreamSerializeDeserialize(t, exp)
}

func TestStream_Serialize_Deserialize_Power(t *testing.T) {
	power := window.NewPowerLengthsSequence(1, 2, 3, 4)
	testStreamSerializeDeserialize(t, power)
}
