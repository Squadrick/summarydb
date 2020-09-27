package stats

import (
	"summarydb/utils"
	"testing"
)

func TestWelford(t *testing.T) {
	welford := NewWelford()

	utils.AssertEqual(t, welford.GetMean(), 0.0)
	utils.AssertEqual(t, welford.GetVariance(), 0.0)
	utils.AssertEqual(t, welford.GetSampleVariance(), 0.0)
	utils.AssertEqual(t, welford.GetCV(), 0.0)

	for i := 1; i < 100; i++ {
		welford.Update(float64(i))
	}

	utils.AssertEqual(t, welford.GetMean(), 50.0)
	utils.AssertClose(t, welford.GetVariance(), 816.666667, 1e-4)
	utils.AssertClose(t, welford.GetSampleVariance(), 825.0000, 1e-4)
	utils.AssertClose(t, welford.GetCV(), 0.5744563, 1e-4)
}
