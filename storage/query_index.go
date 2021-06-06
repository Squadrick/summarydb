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

package storage

import "summarydb/tree"

// In-memory index over window time-starts.
type QueryIndex struct {
	tStarts *tree.RbTree
}

func NewQueryIndex() *QueryIndex {
	return &QueryIndex{tStarts: tree.NewRbTree()}
}

func (index *QueryIndex) GetTree() *tree.RbTree {
	return index.tStarts
}

func (index *QueryIndex) Add(tStart int64) {
	index.tStarts.Insert(tStart, tStart)
}

func (index *QueryIndex) Remove(tStart int64) {
	index.tStarts.Delete(tStart)
}

func (index *QueryIndex) GetNumberWindows() int {
	return index.tStarts.Count()
}

// Get windows that might overlap [ts, ts], specifically
//		[edge window with tStart < ts, ..., edge window with tStart <= te]
// Very first window may not overlap [ts, te], depending on its tEnd.
func (index *QueryIndex) GetOverlappingWindowIDs(t0 int64, t1 int64) []int64 {
	if index.tStarts.IsEmpty() {
		return make([]int64, 0)
	}
	_, l := index.tStarts.Floor(t0)
	if l == nil {
		_, l = index.tStarts.Min()
	}

	_, r := index.tStarts.Ceiling(t1)
	if r == nil {
		_, r = index.tStarts.Max()
	}

	windows := make([]int64, 0, index.GetNumberWindows())
	index.tStarts.Map(func(key tree.RbKey, i interface{}) bool {
		value := i.(int64)
		if value >= l.(int64) && value <= r.(int64) {
			windows = append(windows, i.(int64))
			return false
		} else if value > r.(int64) {
			return true
		}
		return false
	})
	return windows
}
