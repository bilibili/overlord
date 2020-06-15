package create

import (
	"overlord/pkg/types"
	"overlord/platform/chunk"
)

// CacheInfo is the server side create cache info.
type CacheInfo struct {
	JobID string

	Name string

	CacheType types.CacheType

	MaxMemory float64
	CPU       float64
	Number    int

	// for redis : it will be ignore becasue redis only run 1 cpu at all.
	Thread int

	Version string
	Image   string

	Dist  *chunk.Dist
	Group string

	Chunks []*chunk.Chunk
	IDMap  map[string]map[int]string
}
