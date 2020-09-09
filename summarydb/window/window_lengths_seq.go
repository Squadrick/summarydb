package window

import "math"

type LengthsSequence interface {
	NextWindowLength() int64
	MaxWindowSize() int64
}

// 1, base, base^2, ..., base^k, ...
type ExponentialLengthsSequence struct {
	next float64
	base float64
}

func NewExponentialLengthsSequence(base float64) *ExponentialLengthsSequence {
	return &ExponentialLengthsSequence{
		next: 1.0,
		base: base,
	}
}

func (seq *ExponentialLengthsSequence) NextWindowLength() int64 {
	prev := seq.next
	seq.next *= seq.base
	return int64(math.Ceil(prev))
}

func (seq *ExponentialLengthsSequence) MaxWindowSize() int64 {
	return math.MaxUint32
}
