package tree

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestMinHeap(t *testing.T) {
	heap := NewMinHeap(10)

	for i := 9; i >= 0; i-- {
		item := &HeapItem{
			Value:    int64(i + 10),
			Priority: i,
		}
		heap.Push(item)
	}

	itemDel := heap.Top().(*HeapItem)
	heap.Delete(itemDel)

	itemUpdate := heap.Top().(*HeapItem)
	heap.Update(itemUpdate, 999, itemUpdate.Priority)
	assert.Equal(t, heap.Top().(*HeapItem).Value, int64(999))

	for i := 0; i < 10; i++ {
		if itemDel.Priority == i {
			continue
		}
		item := heap.Pop().(*HeapItem)
		assert.Equal(t, item.Priority, i)
		if i == 1 {
			assert.Equal(t, item.Value, int64(999))
		} else {
			assert.Equal(t, item.Value, int64(i+10))
		}
	}
}
