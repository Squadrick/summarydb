package storage

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestGetKey(t *testing.T) {
	a := int64(1>>32 - 1)

	key := GetKey(false, a, a-2)

	streamID := GetStreamIDFromKey(key)
	windowID := GetWindowIDFromKey(key)
	landmark := GetLandmarkFromKey(key)

	assert.Equal(t, streamID, a)
	assert.Equal(t, windowID, a-2)
	assert.Equal(t, landmark, false)
}

func TestGetKeyLandmark(t *testing.T) {
	a := int64(1>>64 - 1)

	key := GetKey(true, a, a-2)

	streamID := GetStreamIDFromKey(key)
	windowID := GetWindowIDFromKey(key)
	landmark := GetLandmarkFromKey(key)

	assert.Equal(t, streamID, a)
	assert.Equal(t, windowID, a-2)
	assert.Equal(t, landmark, true)
}

func TestBadgerBackend_Summary(t *testing.T) {
	testConfig := TestBadgerDB()
	badger := NewBadgerBacked(testConfig)

	window := []byte{0, 1, 2, 3, 4, 5}
	err := badger.Put(12, 34, window)
	assert.NoError(t, err)
	dbWindow, err := badger.Get(12, 34)
	assert.NoError(t, err)

	assert.Equal(t, window, dbWindow)
}

func TestBadgerBackend_IterateIndex(t *testing.T) {
	testConfig := TestBadgerDB()
	badger := NewBadgerBacked(testConfig)
	testIterateIndex(t, badger)
}
