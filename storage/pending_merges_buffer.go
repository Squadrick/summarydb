package storage

type PendingMergeBuffer struct {
	Id           int64
	MergedWindow []byte
	DeletedIDs   []int64
}
