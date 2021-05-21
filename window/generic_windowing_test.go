package window

import (
	"github.com/stretchr/testify/assert"
	"math"
	"summarydb/protos"
	"testing"
)

const (
	Multiplier = int64(2492)
)

func TestGenericWindowing_GetFirstContainingTime(t *testing.T) {
	expSeq := NewExponentialLengthsSequence(2)
	window := NewGenericWindowing(expSeq)

	getTime := func(Tl, Tr, T int64) int64 {
		value, ok := window.GetFirstContainingTime(Tl, Tr, T)
		if !ok {
			t.Fatalf("Getting value failed for: [%d, %d, %d]\n", Tl, Tr, T)
		}
		return value
	}

	assert.Equal(t, int64(101), getTime(98, 99, 100))
	assert.Equal(t, int64(103), getTime(96, 99, 100))
	assert.Equal(t, int64(107), getTime(92, 99, 100))
	assert.Equal(t, int64(115), getTime(84, 99, 100))
	assert.Equal(t, int64(200), getTime(80, 100, 200))
}

type TestSeq struct {
	i int64
}

func (seq *TestSeq) NextWindowLength() int64 {
	value := seq.i * Multiplier
	seq.i++
	return value
}

func (seq *TestSeq) MaxWindowSize() int64 {
	return math.MaxUint32
}

func (seq *TestSeq) Serialize(*protos.Stream_window) {}
func (seq *TestSeq) Deserialize(*protos.Stream_window) {}
func (seq *TestSeq) Equals(sequence LengthsSequence) bool {
	return true
}

func TestGenericWindowing_GetSizeOfFirstWindow(t *testing.T) {
	window := NewGenericWindowing(NewExponentialLengthsSequence(2))

	assert.Equal(t, window.GetSizeOfFirstWindow(), int64(1))
	rp := NewGenericWindowing(&TestSeq{i: 1})
	assert.Equal(t, Multiplier, rp.GetSizeOfFirstWindow())
}

func TestGenericWindowing_GetWindowsCoveringUpto(t *testing.T) {
	window := NewGenericWindowing(NewExponentialLengthsSequence(2))

	assert.Equal(t, window.GetWindowsCoveringUpto(62), []int64{1, 2, 4, 8, 16})
	assert.Equal(t, window.GetWindowsCoveringUpto(63), []int64{1, 2, 4, 8, 16, 32})
}

func benchmarkGetWindowsCoveringUpto(input int, b *testing.B) {
	for n := 0; n < b.N; n++ {
		window := NewGenericWindowing(NewExponentialLengthsSequence(2))
		_ = window.GetWindowsCoveringUpto(int64(input))
	}
}

func BenchmarkGenericWindowing_GetWindowsCoveringUpto10(b *testing.B) {
	benchmarkGetWindowsCoveringUpto(10, b)
}
func BenchmarkGenericWindowing_GetWindowsCoveringUpto100(b *testing.B) {
	benchmarkGetWindowsCoveringUpto(100, b)
}
func BenchmarkGenericWindowing_GetWindowsCoveringUpto1000(b *testing.B) {
	benchmarkGetWindowsCoveringUpto(1000, b)
}
func BenchmarkGenericWindowing_GetWindowsCoveringUpto10000(b *testing.B) {
	benchmarkGetWindowsCoveringUpto(10000, b)
}
