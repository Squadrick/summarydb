package window

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestPowerLengthsSequence_NextWindowLength(t *testing.T) {
	seq := NewPowerLengthsSequence(1, 1, 10, 1)

	for i := 0; i < 100; i++ {
		assert.Equal(t, int64(i/10), seq.NextWindowLength()-1)
	}
}
