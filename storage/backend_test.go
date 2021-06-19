package storage

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func testIterateIndex(t *testing.T, backend Backend) {
	err := backend.Put(1, 1, nil)
	assert.NoError(t, err)
	err = backend.Put(1, 2, nil)
	assert.NoError(t, err)
	err = backend.Put(2, 3, nil)
	assert.NoError(t, err)
	err = backend.Put(2, 4, nil)
	assert.NoError(t, err)
	err = backend.PutLandmark(1, 5, nil)
	assert.NoError(t, err)
	err = backend.PutLandmark(1, 6, nil)
	assert.NoError(t, err)

	var index []int64

	initIndex := func() {
		index = make([]int64, 0)
	}

	lambda := func(windowID int64) error {
		index = append(index, windowID)
		return nil
	}

	initIndex()
	err = backend.IterateIndex(1, lambda, false)
	assert.NoError(t, err)
	assert.Contains(t, index, int64(1))
	assert.Contains(t, index, int64(2))

	initIndex()
	err = backend.IterateIndex(2, lambda, false)
	assert.NoError(t, err)
	assert.Contains(t, index, int64(3))
	assert.Contains(t, index, int64(4))

	initIndex()
	err = backend.IterateIndex(1, lambda, true)
	assert.NoError(t, err)
	assert.Contains(t, index, int64(5))
	assert.Contains(t, index, int64(6))

	initIndex()
	err = backend.IterateIndex(2, lambda, true)
	assert.NoError(t, err)
	assert.Empty(t, index)
}

func TestInMemoryBackend_IterateIndex(t *testing.T) {
	backend := NewInMemoryBackend()
	testIterateIndex(t, backend)
}
