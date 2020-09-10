/*
* Copyright 2020 Dheeraj R. Reddy.
*
* Copyright 2016 Samsung Research America. All rights reserved.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* This file has been modified by Dheeraj R. Reddy by being re-written
* in Golang.
 */

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
