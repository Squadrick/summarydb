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
	"math"
	"summarystore/tree"
)

// S is the fixed size of the starting window
// Initially, we have R windows each of size S
// Each consequent step will increase both the size of the window, and number of windows
// We use p to denote the exponential rate of increase of number of windows (R)
// We use q to denate the exponential rate of increase of window size (S)
// The exact sequence:
//		(R)           windows of size (S)
//		(R * 2^(p-1)) windows of size (S * 2^q)
//		(R * 3^(p-1)) windows of size (S * 3^q)
//		...
//		(R * k^(p-1)) windows of size (S * k^q)
//		...
// k denotes the current window "iteration" step
//
// The rate of decay, b(n) = O(n^(-q/(p+q)))
//
// NOTE: This can be emulated using `GenericWindowing` and passing an appropriate
// `LengthSeq` interface that has the same sequence as above, but it'll be a lot
// less performant.

type PowerWindowing struct {
	p          int64
	q          int64
	R          int64
	S          int64
	k          int64
	lastLength int64
	lastMarker int64
	// For each distinct length l = S * k^q, k = 1, 2, 3, ... store
	// both l-> left marker of the first window of size l and inverse mapping
	lengthToFirstMarker *tree.RbTree
	firstMarkerToLength *tree.RbTree
}

func NewPowerWindowing(p, q, R, S int64) *PowerWindowing {
	window := &PowerWindowing{
		p:                   p,
		q:                   q,
		R:                   R,
		S:                   S,
		k:                   0,
		lastLength:          0,
		lastMarker:          0,
		lengthToFirstMarker: tree.NewRbTree(),
		firstMarkerToLength: tree.NewRbTree(),
	}
	window.addOne()
	return window
}

func int64Pow(a, b int64) int64 {
	return int64(math.Pow(float64(a), float64(b)))
}

func (pwin *PowerWindowing) addOne() {
	pwin.lastLength = pwin.S * int64Pow(pwin.k+1, pwin.q)
	pwin.lastMarker += pwin.R * int64Pow(pwin.k, pwin.p+pwin.q-1)
	pwin.k++
	lastLengthKey := tree.Int64Key(pwin.lastLength)
	lastMarkerKey := tree.Int64Key(pwin.lastMarker)
	pwin.lengthToFirstMarker.Insert(&lastLengthKey, pwin.lastMarker)
	pwin.firstMarkerToLength.Insert(&lastMarkerKey, pwin.lastLength)
}

func (pwin *PowerWindowing) addUntilLength(targetLength int64) {
	if pwin.q != 0 {
		for pwin.lastLength < targetLength {
			pwin.addOne()
		}
	}
}

func (pwin *PowerWindowing) addPastMarker(targetMarker int64) {
	if pwin.q != 0 {
		for pwin.lastMarker <= targetMarker {
			pwin.addOne()
		}
	}
}

func (pwin *PowerWindowing) GetFirstContainingTime(Tl, Tr, T int64) (int64, bool) {
	// assert 0 <= Tl <= Tr <= T-1
	l := T - 1 - Tr
	r := T - 1 - Tl
	length := Tr - Tl + 1

	if pwin.q == 0 && length > pwin.S {
		// if q = 0, the maximum size of any window is strictly
		// equal to S. Lengths are bounded to q > 0.
		return -1, false
	}

	pwin.addUntilLength(length)
	lengthKey := tree.Int64Key(length)
	lengthMarkerKey, _ := pwin.lengthToFirstMarker.Ceiling(&lengthKey)
	lengthMarker := int64(*lengthMarkerKey.(*tree.Int64Key))
	if lengthMarker >= l {
		return T + lengthMarker - l, true
	}
	// We have already hit the target length, so [l, r] is either
	// already in the same window or will be once we move into next window
	pwin.addPastMarker(l)
	lKey := tree.Int64Key(l)
	targetLengthKey, lengthMarkerValue := pwin.firstMarkerToLength.Floor(&lKey)
	targetLength := int64(*targetLengthKey.(*tree.Int64Key))
	lengthMarker = lengthMarkerValue.(int64)

	// [Wl, Wr] is the window containing l
	Wl := lengthMarker + (l-lengthMarker)/targetLength
	Wr := Wl + targetLength - 1
	if r <= Wr {
		return T, true
	} else {
		// TODO(squadrick): Find case that hits this.
		return T + Wr + 1 - l, true
	}
}

func (pwin *PowerWindowing) GetSizeOfFirstWindow() int64 {
	return pwin.S
}

func (pwin *PowerWindowing) GetWindowsCoveringUpto(n int64) []int64 {
	if n <= 0 {
		return make([]int64, 0)
	}
	windows := make([]int64, 0)
	nSoFar := int64(0)
	for k := int64(1); ; k++ {
		count := pwin.R * int64Pow(k, pwin.p-1)
		size := pwin.S * int64Pow(k, pwin.q)
		for i := int64(0); i < count; i++ {
			if nSoFar+size > n {
				return windows
			} else {
				windows = append(windows, size)
				nSoFar += size
			}
		}
	}
}
