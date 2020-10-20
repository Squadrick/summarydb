package tree

import (
	"container/heap"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestMinHeap(t *testing.T) {
	minHeap := NewMinHeap(10)

	for i := 9; i >= 0; i-- {
		item := &HeapItem{
			Value:    int64(i + 10),
			Priority: i,
		}
		heap.Push(minHeap, item)
	}

	itemDel := minHeap.Top().(*HeapItem)
	assert.Equal(t, itemDel.Priority, 0)
	heap.Remove(minHeap, itemDel.Index)

	itemUpdate := minHeap.Top().(*HeapItem)
	minHeap.Update(itemUpdate, 999, itemUpdate.Priority)
	assert.Equal(t, minHeap.Top().(*HeapItem).Value, int64(999))

	for i := 0; i < 10; i++ {
		if itemDel.Priority == i {
			continue
		}
		item := heap.Pop(minHeap).(*HeapItem)
		assert.Equal(t, item.Priority, i)
		if i == 1 {
			assert.Equal(t, item.Value, int64(999))
		} else {
			assert.Equal(t, item.Value, int64(i+10))
		}
	}
}
