package proto

import (
	"overlord/pkg/types"
	"time"
)

// SlowlogEntries is the struct with additional infomation of slowlog
type SlowlogEntries struct {
	Cluster string
	Entries []*SlowlogEntry
}

// SlowlogEntry is each slowlog item
type SlowlogEntry struct {
	CacheType types.CacheType
	Cmd       [][]byte

	StartTime time.Time
	TotalDur  time.Duration
	RemoteDur time.Duration
	Subs      []*SlowlogEntry
}
