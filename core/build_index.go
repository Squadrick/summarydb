package core

import "summarydb/storage"

// Given empty indices and an existing Backed, we'll populate the indices.
// We do not persist the index to disk. It will be built when starting the database.

func PopulateBackendIndex(backend storage.Backend,
	summaryIndex *storage.QueryIndex,
	landmarkIndex *storage.QueryIndex,
	streamID int64) error {

	generateInsertionLambda := func(index *storage.QueryIndex) func(int64) error {
		insertToIndex := func(windowID int64) error {
			index.Add(windowID)
			return nil
		}
		return insertToIndex
	}
	err := backend.IterateIndex(
		streamID, generateInsertionLambda(summaryIndex), false)
	if err != nil {
		return err
	}
	err = backend.IterateIndex(
		streamID, generateInsertionLambda(landmarkIndex), true)
	if err != nil {
		return err
	}
	return err
}
