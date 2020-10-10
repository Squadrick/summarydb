package stats

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestWelford(t *testing.T) {
	welford := NewWelford()

	assert.Equal(t, welford.GetMean(), 0.0)
	assert.Equal(t, welford.GetVariance(), 0.0)
	assert.Equal(t, welford.GetSampleVariance(), 0.0)
	assert.Equal(t, welford.GetCV(), 0.0)

	for i := 1; i < 100; i++ {
		welford.Update(float64(i))
	}

	assert.InEpsilon(t, welford.GetMean(), 50.0, 1e-4)
	assert.InEpsilon(t, welford.GetVariance(), 816.666667, 1e-4)
	assert.InEpsilon(t, welford.GetSampleVariance(), 825.0000, 1e-4)
	assert.InEpsilon(t, welford.GetCV(), 0.5744563, 1e-4)
}
