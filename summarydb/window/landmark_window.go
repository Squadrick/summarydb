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

import "summarystore/tree"

type LandmarkWindow struct {
	timeStart int64
	timeEnd   int64
	values    *tree.RbTree
}

func NewLandmarkWindow(timeStart int64) *LandmarkWindow {
	return &LandmarkWindow{
		timeStart: timeStart,
		timeEnd:   0,
		values:    tree.NewRbTree(),
	}
}

func (window *LandmarkWindow) Close(timestamp int64) {
	// assert timestamp >= ts and (values.Empty() or values.LastEntry() <= timeStamp)
	window.timeEnd = timestamp
}

func (window *LandmarkWindow) Insert(timestamp int64, value interface{}) {
	// assert timestamp >= ts and (values.Empty() or values.LastEntry() < timeStamp)
	timestampKey := tree.Int64Key(timestamp)
	window.values.Insert(&timestampKey, value)
}
