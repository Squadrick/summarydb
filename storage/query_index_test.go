package storage

import (
	"summarydb/utils"
	"testing"
)

func TestQueryIndex_GetOverlappingWindowIDs(t *testing.T) {
	arrayEqual := func(a, b []int64) bool {
		if (a == nil) != (b == nil) {
			return false
		}

		if len(a) != len(b) {
			return false
		}

		for i := range a {
			if a[i] != b[i] {
				return false
			}
		}
		return true
	}
	index := NewQueryIndex()

	for i := 0; i < 5; i++ {
		index.Add(int64(i * 5))
	}

	windows := index.GetOverlappingWindowIDs(8, 15)
	utils.AssertTrue(t, arrayEqual(windows, []int64{5, 10, 15}))
	index.Remove(15)
	windows = index.GetOverlappingWindowIDs(5, 15)
	utils.AssertTrue(t, arrayEqual(windows, []int64{5, 10, 20}))
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
