package core

import "summarydb/storage"

type StoreConfig struct {
	EachBufferSize  int64
	NumBuffer       int64
	WindowsPerMerge int64
	BadgerConfig    *storage.BadgerBackendConfig
}
