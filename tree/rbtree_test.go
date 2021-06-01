package tree

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestInsertDeleteAndGet(t *testing.T) {
	tree := NewRbTree()
	zeroKey := int64(0)
	for i := 0; i < 25; i++ {
		key := int64(i)
		tree.Insert(key, 10+i)
	}

	for i := 50; i < 100; i += 2 {
		key := int64(i)
		tree.Insert(key, 10+i)
	}

	for i := 51; i < 100; i += 2 {
		key := int64(i)
		tree.Insert(key, 10+i)
	}

	for i := 49; i >= 25; i-- {
		key := int64(i)
		tree.Insert(key, 10+i)
	}

	assert.Equal(t, tree.Count(), 100)

	tree.Insert(zeroKey, 999)

	_, minValue := tree.Min()
	assert.Equal(t, minValue.(int), 999)
	_, maxValue := tree.Max()
	assert.Equal(t, maxValue.(int), 109)

	fiftyKey := int64(50)
	tree.Delete(fiftyKey)

	_, floorVal := tree.Floor(fiftyKey)
	assert.Equal(t, floorVal.(int), 59)
	_, ceilVal := tree.Ceiling(fiftyKey)
	assert.Equal(t, ceilVal.(int), 61)

	tree.Insert(fiftyKey, 60)
	_, higherVal := tree.Higher(fiftyKey)
	assert.Equal(t, higherVal.(int), 61)

	count := 0
	for i := 1; i < 150; i++ {
		key := int64(i)
		if value, ok := tree.Get(key); ok {
			assert.Equal(t, value.(int), i+10)
			count++
		}
	}
	assert.Equal(t, count, 99) // all but 0

	denseMap := tree.GetDenseMap()
	keys := denseMap.GetKeys()
	assert.Equal(t, len(keys), tree.Count())

	for iter, mapKey := range keys {
		value, ok := denseMap.Get(mapKey)
		if !ok {
			t.Fatalf("Lookup failed for: %d\n", iter)
		}
		if iter == 0 {
			assert.Equal(t, 999, value.(int))
		} else {
			assert.Equal(t, iter+10, value.(int))
		}
	}

	sum := 0
	tree.Map(func(key RbKey, i interface{}) bool {
		sum += i.(int)
		if i.(int) == 20 {
			return true
		}
		return false
	})
	assert.Equal(t, sum, 1154)

	for i := 1; i < 100; i++ {
		key := int64(i)
		tree.Delete(key)
	}

	if tree.IsEmpty() {
		t.Fatalf("Tree is empty\n")
	}

	value, ok := tree.Get(zeroKey)
	if !ok {
		t.Fatalf("Could not get remaining key\n")
	}
	assert.Equal(t, value.(int), 999)

	tree.Delete(zeroKey)

	if !tree.IsEmpty() {
		t.Fatalf("Tree is not empty\n")
	}
}
