package core

import (
	"container/heap"
	"github.com/stretchr/testify/assert"
	"math"
	"strconv"
	"summarydb/storage"
	"summarydb/tree"
	"testing"
)

func GetSummaryWindow() *SummaryWindow {
	window := NewSummaryWindow(1, 2, 3, 4)
	window.Data.Max.Value = 12.12
	window.Data.Count.Value = 13.13
	window.Data.Count.Value = 14.14
	return window
}

func GetLandmarkWindow() *LandmarkWindow {
	window := NewLandmarkWindow(3)
	window.Insert(4, 1.2)
	window.Insert(5, 1.6)
	window.Insert(6, 2.0)
	window.Close(10)
	return window
}

func TestSummaryWindowSerialization(t *testing.T) {
	window := GetSummaryWindow()
	buf := SummaryWindowToBytes(window)
	newWindow := BytesToSummaryWindow(buf)

	assert.Equal(t, window, newWindow)
}

func TestLandmarkWindowSerialization(t *testing.T) {
	window := GetLandmarkWindow()
	buf := LandmarkWindowToBytes(window)
	newWindow := BytesToLandmarkWindow(buf)

	assert.Equal(t, window, newWindow)
}

func TestInMemory(t *testing.T) {
	summaryWindow := GetSummaryWindow()
	landmarkWindow := GetLandmarkWindow()
	backend := storage.NewInMemoryBackend()
	store := NewBackingStore(backend, false)

	store.Put(0, 1, summaryWindow)
	store.PutLandmark(1, 1, landmarkWindow)

	newSummaryWindow := store.Get(0, 1)
	newLandmarkWindow := store.GetLandmark(1, 1)

	assert.Equal(t, summaryWindow, newSummaryWindow)
	assert.Equal(t, landmarkWindow, newLandmarkWindow)
}

func GetIdentity() func(int) int {
	return func(i int) int {
		return i
	}
}

func generateHeap(heapSize int,
	valueTransform func(int) int,
	priorityTransform func(int) int) *tree.MinHeap {
	newHeap := tree.NewMinHeap(heapSize)

	for i := 0; i < heapSize; i += 1 {
		item := &tree.HeapItem{
			Value:    int64(valueTransform(i)),
			Priority: priorityTransform(i),
			Index:    -1,
		}
		heap.Push(newHeap, item)
	}
	return newHeap
}

func TestHeap(t *testing.T) {
	backend := storage.NewBadgerBacked(storage.TestBadgerDB())
	store := NewBackingStore(backend, false)
	testSize := 1000

	valueTransform := func(i int) int {
		return 2 * i
	}
	priorityTransform := func(i int) int {
		return testSize - i - 1
	}
	{
		diskHeap := generateHeap(testSize,
			valueTransform,
			priorityTransform)
		store.PutHeap(0, diskHeap)
	}
	{
		diskHeap := store.GetHeap(0)
		i := 0
		for diskHeap.Len() != 0 {
			heapItem := heap.Pop(diskHeap).(*tree.HeapItem)
			assert.Equal(t, i, heapItem.Priority)
			assert.Equal(t, int64(valueTransform(priorityTransform(i))), heapItem.Value)
			i += 1
		}
	}
}

// 140ns/item , linear growth.
func BenchmarkHeapToBytes(b *testing.B) {
	for i := 2; i < 6; i += 1 {
		size := int(math.Pow(10.0, float64(i)))
		newHeap := generateHeap(size, GetIdentity(), GetIdentity())
		b.Run(strconv.Itoa(size), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_ = HeapToBytes(newHeap)
			}
		})
	}
}

// Starts at 350ns/item, drops to 170ns/item between
// 100 to 1000 items.
func BenchmarkBytesToHeap(b *testing.B) {
	for i := 2; i < 6; i += 1 {
		size := int(math.Pow(10.0, float64(i)))
		newHeap := generateHeap(size, GetIdentity(), GetIdentity())
		rawBytes := HeapToBytes(newHeap)
		b.Run(strconv.Itoa(size), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_ = BytesToHeap(rawBytes)
			}
		})
	}
}

// Starts at 500ns/item, drops to 300ns/item between
// 100 and 1000 items.
func BenchmarkHeap(b *testing.B) {
	backend := storage.NewInMemoryBackend()
	store := NewBackingStore(backend, false)
	for i := 2; i < 6; i += 1 {
		size := int(math.Pow(10.0, float64(i)))
		newHeap := generateHeap(size, GetIdentity(), GetIdentity())
		b.Run(strconv.Itoa(size), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				store.PutHeap(0, newHeap)
				_ = store.GetHeap(0)
			}
		})
	}
}
