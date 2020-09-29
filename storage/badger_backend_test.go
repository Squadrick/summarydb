package storage

import "testing"

func TestGetBadgerKey(t *testing.T) {
	a := int64(1>>64 - 1)

	_ = GetBadgerKey(true, a, a)
}
