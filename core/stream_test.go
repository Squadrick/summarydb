package core

import (
	"github.com/stretchr/testify/assert"
	"summarydb/window"
	"testing"
)

func testStreamSerializeDeserialize(t *testing.T, seq window.LengthsSequence) {
	windowing := window.NewGenericWindowing(seq)
	stream := NewStreamWithId(0, []string{"count", "max", "sum"},
		windowing)
	bytes, err := stream.Serialize()
	assert.NoError(t, err)

	newStream, err := DeserializeStream(bytes)
	assert.NoError(t, err)

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

func BenchmarkStream_Serialize(b *testing.B) {
	power := window.NewPowerLengthsSequence(1, 2, 3, 4)
	windowing := window.NewGenericWindowing(power)

	stream := NewStreamWithId(0, []string{"count", "max", "sum"},
		windowing)

	for n := 0; n < b.N; n++ {
		_, err := stream.Serialize()
		if err != nil {
			b.FailNow()
		}
	}
}

func BenchmarkStream_Deserialize(b *testing.B) {
	power := window.NewPowerLengthsSequence(1, 2, 3, 4)
	windowing := window.NewGenericWindowing(power)

	stream := NewStreamWithId(0, []string{"count", "max", "sum"},
		windowing)

	bytes, err := stream.Serialize()
	if err != nil {
		b.FailNow()
	}
	for n := 0; n < b.N; n++ {
		_, err := DeserializeStream(bytes)
		if err != nil {
			b.FailNow()
		}
	}
}

func BenchmarkStream_SerializeDeserialize(b *testing.B) {
	power := window.NewPowerLengthsSequence(1, 2, 3, 4)
	windowing := window.NewGenericWindowing(power)

	stream := NewStreamWithId(0, []string{"count", "max", "sum"},
		windowing)

	for n := 0; n < b.N; n++ {
		bytes, err := stream.Serialize()
		if err != nil {
			b.FailNow()
		}
		_, err = DeserializeStream(bytes)
		if err != nil {
			b.FailNow()
		}
	}
}
