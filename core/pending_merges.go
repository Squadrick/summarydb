package core

import "summarydb/storage"

type PendingMerge struct {
	MergedWindow *SummaryWindow
	DeletedIDs   []int64
}

func (pm *PendingMerge) Serialize() (*storage.PendingMergeBuffer, error) {
	buf, err := SummaryWindowToBytes(pm.MergedWindow)
	if err != nil {
		return nil, err
	}
	return &storage.PendingMergeBuffer{
		Id:           pm.MergedWindow.Id(),
		MergedWindow: buf,
		DeletedIDs:   pm.DeletedIDs,
	}, nil
}
