package stats

import "math"

type Welford struct {
	count uint64
	mean  float64
	m2    float64
}

func NewWelford() *Welford {
	return &Welford{
		count: 0,
		mean:  0,
		m2:    0,
	}
}

func (welford *Welford) Update(value float64) {
	welford.count++
	delta := value - welford.mean
	welford.mean += delta / float64(welford.count)
	delta2 := value - welford.mean
	welford.m2 += delta * delta2
}

func (welford *Welford) GetMean() float64 {
	return welford.mean
}

func (welford *Welford) GetVariance() float64 {
	if welford.count < 2 {
		return 0
	}
	return welford.m2 / float64(welford.count)
}

func (welford *Welford) GetSampleVariance() float64 {
	if welford.count < 2 {
		return 0
	}
	return welford.m2 / float64(welford.count-1)
}

func (welford *Welford) GetSD() float64 {
	return math.Sqrt(welford.GetSampleVariance())
}

func (welford *Welford) GetCV() float64 {
	if welford.count < 2 {
		return 0
	}
	return welford.GetSD() / welford.GetMean()
}
