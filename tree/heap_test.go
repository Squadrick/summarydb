package tree

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestMinHeap(t *testing.T) {
	heap := NewMinHeap()

	for i := 9; i >= 0; i-- {
		item := &Item{
			value:    int64(i + 10),
			priority: i,
		}
		heap.Push(item)
	}

	itemDel := heap.Top().(*Item)
	heap.Delete(itemDel)

	itemUpdate := heap.Top().(*Item)
	heap.Update(itemUpdate, 999, itemUpdate.priority)
	assert.Equal(t, heap.Top().(*Item).value, int64(999))

	for i := 0; i < 10; i++ {
		if itemDel.priority == i {
			continue
		}
		item := heap.Pop().(*Item)
		assert.Equal(t, item.priority, i)
		if i == 1 {
			assert.Equal(t, item.value, int64(999))
		} else {
			assert.Equal(t, item.value, int64(i+10))
		}
	}
}
