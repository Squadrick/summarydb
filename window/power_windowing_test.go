package window

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestPowerWindowing_GetFirstContainingTime(t *testing.T) {
	window := NewPowerWindowing(1, 2, 1, 1)

	getTime := func(Tl, Tr, T int64) int64 {
		value, ok := window.GetFirstContainingTime(Tl, Tr, T)
		if !ok {
			t.Fatalf("Getting value failed for: [%d, %d, %d]\n", Tl, Tr, T)
		}
		return value
	}

	assert.Equal(t, int64(104), getTime(98, 99, 100))
	assert.Equal(t, int64(104), getTime(98, 99, 100))
	assert.Equal(t, int64(104), getTime(96, 99, 100))
	assert.Equal(t, int64(109), getTime(92, 99, 100))
	assert.Equal(t, int64(116), getTime(84, 99, 100))
	assert.Equal(t, int64(200), getTime(80, 100, 200))
}

func TestPowerWindowing_GetSizeOfFirstWindow(t *testing.T) {
	window := NewPowerWindowing(1, 1, 1, 1337)
	assert.Equal(t, window.GetSizeOfFirstWindow(), int64(1337))
}

func TestPowerWindowing_GetWindowsCoveringUpto(t *testing.T) {
	window := NewPowerWindowing(2, 2, 2, 3)

	assert.Equal(t, window.GetWindowsCoveringUpto(62), []int64{3, 3, 12, 12, 12, 12})
	assert.Equal(t, window.GetWindowsCoveringUpto(100), []int64{3, 3, 12, 12, 12, 12, 27})
}
