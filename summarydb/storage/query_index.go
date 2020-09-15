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

import "summarystore/tree"

// In-memory index over window time-starts.
type QueryIndex struct {
	tStarts *tree.RbTree
}

func NewQueryIndex() *QueryIndex {
	return &QueryIndex{tStarts: tree.NewRbTree()}
}

func (index *QueryIndex) Add(tStart int64) {
	tStartKey := tree.Int64Key(tStart)
	index.tStarts.Insert(&tStartKey, tStart)
}

func (index *QueryIndex) Remove(tStart int64) {
	tStartKey := tree.Int64Key(tStart)
	index.tStarts.Delete(&tStartKey)
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
	t0Key := tree.Int64Key(t0)
	t1Key := tree.Int64Key(t1)
	_, l := index.tStarts.Floor(&t0Key)
	_, r := index.tStarts.Ceiling(&t1Key)
	windows := make([]int64, 0, index.GetNumberWindows())
	windows = append(windows, l.(int64))
	index.tStarts.Map(func(key tree.RbKey, i interface{}) bool {
		value := i.(int64)
		if value > r.(int64) {
			return true
		} else if value > l.(int64) && value <= r.(int64) {
			windows = append(windows, value)
		}
		return false
	})
	return windows
}
