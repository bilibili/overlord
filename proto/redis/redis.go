package redis

import (
	"bytes"
)

// MergeType is used to decript the merge operation.
type MergeType = uint8

// merge types
const (
	MergeTypeCount MergeType = iota
	MergeTypeOk
	MergeTypeJoin
	MergeTypeBasic
)

func getMergeType(cmd []byte) MergeType {
	// TODO: impl with tire tree to search quickly
	if bytes.Equal(cmd, cmdMGetBytes) {
		return MergeTypeJoin
	}
	if bytes.Equal(cmd, cmdMSetBytes) {
		return MergeTypeOk
	}

	if bytes.Equal(cmd, cmdExistsBytes) || bytes.Equal(cmd, cmdDelBytes) {
		return MergeTypeCount
	}

	return MergeTypeBasic
}
