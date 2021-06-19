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

import (
	"errors"
	"math"
	"summarydb/protos"
)

type LengthsSequence interface {
	NextWindowLength() int64
	MaxWindowSize() int64
	Serialize(window *protos.Stream_window) error
	Deserialize(window *protos.Stream_window) error
	Equals(other LengthsSequence) bool
}

func DeserializeLengthsSequence(window *protos.Stream_window) (LengthsSequence, error) {
	var seq LengthsSequence
	if window.Which() == protos.Stream_window_Which_exp {
		seq = NewExponentialLengthsSequence(0)
	} else if window.Which() == protos.Stream_window_Which_power {
		seq = NewPowerLengthsSequence(0, 0, 0, 0)
	} else {
		return nil, errors.New("unknown length sequence")
	}
	err := seq.Deserialize(window)
	if err != nil {
		return nil, err
	}
	return seq, nil
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

func (seq *ExponentialLengthsSequence) Serialize(windowProto *protos.Stream_window) error {
	proto, err := windowProto.NewExp()
	if err != nil {
		return err
	}
	//proto.SetNext(seq.next)
	proto.SetBase(seq.base)
	return nil
}

func (seq *ExponentialLengthsSequence) Deserialize(windowProto *protos.Stream_window) error {
	expProto, err := windowProto.Exp()
	if err != nil {
		return err
	}
	//seq.next = expProto.Next()
	seq.base = expProto.Base()
	return nil
}

func (seq *ExponentialLengthsSequence) Equals(other LengthsSequence) bool {
	switch exp := other.(type) {
	case *ExponentialLengthsSequence:
		return seq.base == exp.base
	default:
		return false
	}
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

func (seq *PowerLengthsSequence) Serialize(windowProto *protos.Stream_window) error {
	proto, err := windowProto.NewPower()
	if err != nil {
		return err
	}
	proto.SetP(seq.p)
	proto.SetQ(seq.q)
	proto.SetR(seq.R)
	proto.SetS(seq.S)
	return nil
}

func (seq *PowerLengthsSequence) Deserialize(windowProto *protos.Stream_window) error {
	powerProto, err := windowProto.Power()
	if err != nil {
		return err
	}
	seq.p = powerProto.P()
	seq.q = powerProto.Q()
	seq.R = powerProto.R()
	seq.S = powerProto.S()
	return nil
}

func (seq *PowerLengthsSequence) Equals(other LengthsSequence) bool {
	switch power := other.(type) {
	case *PowerLengthsSequence:
		return seq.p == power.p &&
			seq.q == power.q &&
			seq.R == power.R &&
			seq.S == power.S
	default:
		return false
	}
}
