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

type PowerLengthsSequence struct {
	p    int64
	q    int64
	R    int64
	S    int64
	k    int64
	curr int64
}

func NewPowerLengthsSequence(p, q, R, S int64) *PowerLengthsSequence {
	return &PowerLengthsSequence{
		p:    p,
		q:    q,
		R:    R,
		S:    S,
		k:    1,
		curr: 0,
	}
}

func (seq *PowerLengthsSequence) NextWindowLength() int64 {
	count := seq.R * int64Pow(seq.k, seq.p-1)
	if count <= seq.curr {
		seq.k++
		seq.curr = 0
	}
	seq.curr++
	return seq.S * int64Pow(seq.k, seq.q)
}

func (seq *PowerLengthsSequence) MaxWindowSize() int64 {
	return math.MaxUint32
}
