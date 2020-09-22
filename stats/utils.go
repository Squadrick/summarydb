package stats

import (
	"math"
)

func Int64Max(a, b int64) int64 {
	return int64(math.Max(float64(a), float64(b)))
}

func Int64Min(a, b int64) int64 {
	return int64(math.Min(float64(a), float64(b)))
}

func WindowLength(l, r int64) int64 {
	return r - l + 1
}

// How much does [l1, r1] and [l2, r2] overlap?
func WindowOverlap(l1, r1, l2, r2 int64) int64 {
	return Int64Max(Int64Min(r1, r2)-Int64Max(l1, l2)+1, 0)
}

type Bounds struct {
	Lower float64
	Upper float64
}

type Stats struct {
	Mean float64
	Var  float64
}

type CI struct {
	Mean    float64
	LowerCI float64
	UpperCI float64
}

func ConvertStatsBoundsToCI(bounds *Bounds, stats *Stats, sdMultiplier, confidenceLevel float64) *CI {
	ci := &CI{
		Mean: stats.Mean,
	}
	probability := (1 + confidenceLevel) / 2
	z := StdNormal.InvCDF(probability)

	if math.IsInf(z, 0) {
		ci.LowerCI = bounds.Lower
		ci.UpperCI = bounds.Upper
	} else {
		sd := sdMultiplier * math.Sqrt(stats.Var)
		ci.LowerCI = math.Max(ci.Mean-z*sd, bounds.Lower)
		ci.UpperCI = math.Min(ci.Mean+z*sd, bounds.Lower)
	}
	return ci
}
