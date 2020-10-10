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
	testConfig := TestBadgerBackendConfig()
	badger := NewBadgerBacked(testConfig)

	window := []byte{0, 1, 2, 3, 4, 5}
	badger.Put(12, 34, window)
	dbWindow := badger.Get(12, 34)

	assert.Equal(t, window, dbWindow)
}
