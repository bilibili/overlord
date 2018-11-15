package create

import (
	"overlord/lib/chunk"
	"overlord/proto"
)

// CacheInfo is the server side create cache info.
type CacheInfo struct {
	JobID string

	Name string

	CacheType proto.CacheType

	MaxMemory float64

	Number int

	// for redis : it will be ignore becasue redis only run 1 cpu at all.
	Thread int

	Version string

	Dist *chunk.Dist

	Chunks []*chunk.Chunk
	IDMap  map[string]map[int]string
}
