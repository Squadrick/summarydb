package core

import "summarydb/storage"

type StoreConfig struct {
	TotalBufferSize int64
	NumBuffer       int64
	WindowsPerBatch int64
	BadgerConfig    *storage.BadgerBackendConfig
}
