package core

import "summarydb/storage"

// Given empty indices and an existing Backed, we'll populate the indices.
// We do not persist the index to disk. It will be built when starting the database.

func PopulateIndex(backend storage.Backend,
	summaryIndex *storage.QueryIndex,
	landmarkIndex *storage.QueryIndex,
	streamID int64) {

	generateInsertionLambda := func(index *storage.QueryIndex) func(int64) {
		insertToIndex := func(windowID int64) {
			index.Add(windowID)
		}
		return insertToIndex
	}
	backend.IterateIndex(streamID, generateInsertionLambda(summaryIndex), false)
	backend.IterateIndex(streamID, generateInsertionLambda(landmarkIndex), true)
}
