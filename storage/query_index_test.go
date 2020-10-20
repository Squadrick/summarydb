package storage

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestQueryIndex_GetOverlappingWindowIDs(t *testing.T) {
	index := NewQueryIndex()

	for i := 0; i < 5; i++ {
		index.Add(int64(i * 5))
	}

	windows := index.GetOverlappingWindowIDs(8, 15)
	assert.Equal(t, windows, []int64{5, 10, 15})
	index.Remove(15)
	windows = index.GetOverlappingWindowIDs(5, 15)
	assert.Equal(t, windows, []int64{5, 10, 20})
}

func TestQueryIndex_GetOverlappingWindowIDs_Empty(t *testing.T) {
	index := NewQueryIndex()
	index.Add(int64(0))
	windows := index.GetOverlappingWindowIDs(0, 1)
	assert.Equal(t, windows, []int64{0})
}

func benchmarkGetOverlappingWindowIDs(b *testing.B, count int) {
	index := NewQueryIndex()
	for i := 0; i < count; i++ {
		index.Add(int64(i) * 5)
	}

	for i := 0; i < count; i += 3 {
		index.Remove(int64(i) * 5)
	}

	t0 := 0.25 * 5 * float32(count)
	t1 := 0.75 * 5 * float32(count)

	for n := 0; n < b.N; n++ {
		_ = index.GetOverlappingWindowIDs(int64(t0), int64(t1))
	}
}

func BenchmarkQueryIndex_GetOverlappingWindowIDs100(b *testing.B) {
	benchmarkGetOverlappingWindowIDs(b, 100)
}
func BenchmarkQueryIndex_GetOverlappingWindowIDs1000(b *testing.B) {
	benchmarkGetOverlappingWindowIDs(b, 1000)
}
func BenchmarkQueryIndex_GetOverlappingWindowIDs10000(b *testing.B) {
	benchmarkGetOverlappingWindowIDs(b, 10000)
}
func BenchmarkQueryIndex_GetOverlappingWindowIDs100000(b *testing.B) {
	benchmarkGetOverlappingWindowIDs(b, 100000)
}
