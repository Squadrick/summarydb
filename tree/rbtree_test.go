package tree

import (
	"testing"
)

func assertEqual(t *testing.T, a interface{}, b interface{}) {
	if a != b {
		t.Fatalf("Expected equal: %s != %s\n", a, b)
	}
}

func TestInsertDeleteAndGet(t *testing.T) {
	tree := NewRbTree()
	zeroKey := Int64Key(0)
	for i := 0; i < 25; i++ {
		key := Int64Key(i)
		tree.Insert(&key, 10+i)
	}

	for i := 50; i < 100; i += 2 {
		key := Int64Key(i)
		tree.Insert(&key, 10+i)
	}

	for i := 51; i < 100; i += 2 {
		key := Int64Key(i)
		tree.Insert(&key, 10+i)
	}

	for i := 49; i >= 25; i-- {
		key := Int64Key(i)
		tree.Insert(&key, 10+i)
	}

	assertEqual(t, tree.Count(), 100)

	tree.Insert(&zeroKey, 999)

	_, minValue := tree.Min()
	assertEqual(t, minValue.(int), 999)
	_, maxValue := tree.Max()
	assertEqual(t, maxValue.(int), 109)

	fiftyKey := Int64Key(50)
	tree.Delete(&fiftyKey)

	_, floorVal := tree.Floor(&fiftyKey)
	assertEqual(t, floorVal.(int), 59)
	_, ceilVal := tree.Ceiling(&fiftyKey)
	assertEqual(t, ceilVal.(int), 61)

	tree.Insert(&fiftyKey, 60)
	_, higherVal := tree.Higher(&fiftyKey)
	assertEqual(t, higherVal.(int), 61)

	count := 0
	for i := 1; i < 150; i++ {
		key := Int64Key(i)
		if value, ok := tree.Get(&key); ok {
			assertEqual(t, value.(int), i+10)
			count++
		}
	}
	assertEqual(t, count, 99) // all but 0

	denseMap := tree.GetDenseMap()
	keys := denseMap.GetKeys()
	assertEqual(t, len(keys), tree.Count())

	for iter, mapKey := range keys {
		value, ok := denseMap.Get(mapKey)
		if !ok {
			t.Fatalf("Lookup failed for: %d\n", iter)
		}
		if iter == 0 {
			assertEqual(t, 999, value.(int))
		} else {
			assertEqual(t, iter+10, value.(int))
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
	assertEqual(t, sum, 1154)

	for i := 1; i < 100; i++ {
		key := Int64Key(i)
		tree.Delete(&key)
	}

	if tree.IsEmpty() {
		t.Fatalf("Tree is empty\n")
	}

	value, ok := tree.Get(&zeroKey)
	if !ok {
		t.Fatalf("Could not get remaining key\n")
	}
	assertEqual(t, value.(int), 999)

	tree.Delete(&zeroKey)

	if !tree.IsEmpty() {
		t.Fatalf("Tree is not empty\n")
	}
}
