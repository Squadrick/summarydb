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

import "summarydb/tree"

type GenericWindowing struct {
	lengthSeq             LengthsSequence
	firstWindowOfLength   *tree.RbTree
	windowStartMarkersSet *tree.RbTree
	firstWindowLength     int64
	lastWindowStart       int64
	lastWindowLength      int64
}

func NewGenericWindowing(lengthSeq LengthsSequence) *GenericWindowing {
	genericWindow := &GenericWindowing{
		lengthSeq:             lengthSeq,
		firstWindowOfLength:   tree.NewRbTree(),
		windowStartMarkersSet: tree.NewRbTree(),
		firstWindowLength:     lengthSeq.NextWindowLength(),
		lastWindowStart:       0,
		lastWindowLength:      0,
	}
	genericWindow.addWindow(genericWindow.firstWindowLength)
	return genericWindow
}

func (gwin *GenericWindowing) addWindow(length int64) {
	gwin.lastWindowStart += gwin.lastWindowLength
	if length > gwin.lastWindowLength {
		lengthKey := tree.Int64Key(length)
		gwin.firstWindowOfLength.Insert(&lengthKey, gwin.lastWindowStart)
	}
	lastWindowStartKey := tree.Int64Key(gwin.lastWindowStart)
	gwin.windowStartMarkersSet.Insert(&lastWindowStartKey, gwin.lastWindowStart)
	gwin.lastWindowLength = length
}

// Add windows until we have one length >= target. Returns false if target
// length is not achievable.
func (gwin *GenericWindowing) addWindowsUntilLength(targetLength int64) bool {
	if targetLength > gwin.lengthSeq.MaxWindowSize() {
		return false
	} else {
		for gwin.lastWindowLength < targetLength {
			gwin.addWindow(gwin.lengthSeq.NextWindowLength())
		}
		return true
	}
}

// Add windows until we have at least one window marker larger than target.
func (gwin *GenericWindowing) addWindowsPastMarker(targetMarker int64) {
	for gwin.lastWindowStart <= targetMarker {
		gwin.addWindow(gwin.lengthSeq.NextWindowLength())
	}
}

// Add windows until we have at least the specified count
func (gwin *GenericWindowing) addWindowsUntilCount(count int) {
	for gwin.windowStartMarkersSet.Count() < count {
		gwin.addWindow(gwin.lengthSeq.NextWindowLength())
	}
}

func (gwin *GenericWindowing) GetFirstContainingTime(Tl, Tr, T int64) (int64, bool) {
	l := T - 1 - Tr
	r := T - 1 - Tl
	length := Tr - Tl + 1

	if !gwin.addWindowsUntilLength(length) {
		return 0, false
	}

	lengthKey := tree.Int64Key(length)
	_, firstMarker := gwin.firstWindowOfLength.Ceiling(&lengthKey)
	firstMarkerValue, ok := firstMarker.(int64)
	if !ok {
		return 0, false
	}

	if firstMarkerValue >= l {
		// l' == firstMarkerValue, where l' = N' - 1 - Tr
		return firstMarkerValue + Tr + 1, true
	}

	// We've already hit the target window length, so [l, r] is either
	// already in the same window or will be once we move into the next window
	gwin.addWindowsPastMarker(l)
	lKey := tree.Int64Key(l)
	currWindowLKey, _ := gwin.windowStartMarkersSet.Floor(&lKey)
	currWindowRKey, _ := gwin.windowStartMarkersSet.Higher(&lKey)
	currWindowL := int64(*currWindowLKey.(*tree.Int64Key))
	currWindowR := int64(*currWindowRKey.(*tree.Int64Key))

	if r <= currWindowR {
		// already in the same window
		return T, true
	} else {
		// TODO(squadrick): Find example that hits this case
		if currWindowR-currWindowL+1 < length {
			return 0, false
		}

		// need to wait until next window, i.e l' == currWindowR + 1, l' = N' - 1 - Tr
		return currWindowR + Tr + 2, ok
	}
}

func (gwin *GenericWindowing) GetSizeOfFirstWindow() int64 {
	return gwin.firstWindowLength
}

func (gwin *GenericWindowing) GetWindowsCoveringUpto(n int64) []int64 {
	if n <= 0 {
		return make([]int64, 0)
	}

	gwin.addWindowsPastMarker(n)

	windows := make([]int64, 0, gwin.windowStartMarkersSet.Count())
	prevMarker := int64(0)

	gwin.windowStartMarkersSet.Map(func(_ tree.RbKey, value interface{}) bool {
		currentMarker := value.(int64)
		if currentMarker <= n {
			if currentMarker != 0 {
				windows = append(windows, currentMarker-prevMarker)
				prevMarker = currentMarker
			}
			return false
		}
		return true
	})
	return windows
}
