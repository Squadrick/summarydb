package storage

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func testIterateIndex(t *testing.T, backend Backend) {
	backend.Put(1, 1, nil)
	backend.Put(1, 2, nil)
	backend.Put(2, 3, nil)
	backend.Put(2, 4, nil)
	backend.PutLandmark(1, 5, nil)
	backend.PutLandmark(1, 6, nil)

	var index []int64

	initIndex := func() {
		index = make([]int64, 0)
	}

	lambda := func(windowID int64) error {
		index = append(index, windowID)
		return nil
	}

	initIndex()
	backend.IterateIndex(1, lambda, false)
	assert.Contains(t, index, int64(1))
	assert.Contains(t, index, int64(2))

	initIndex()
	backend.IterateIndex(2, lambda, false)
	assert.Contains(t, index, int64(3))
	assert.Contains(t, index, int64(4))

	initIndex()
	backend.IterateIndex(1, lambda, true)
	assert.Contains(t, index, int64(5))
	assert.Contains(t, index, int64(6))

	initIndex()
	backend.IterateIndex(2, lambda, true)
	assert.Empty(t, index)
}

func TestInMemoryBackend_IterateIndex(t *testing.T) {
	backend := NewInMemoryBackend()
	testIterateIndex(t, backend)
}
