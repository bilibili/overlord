package proto

import (
	"time"

	"overlord/pkg/types"
)

const copyCollapsedMaxLength = 256 - 3

// SlowlogEntries is the struct with additional infomation of slowlog
type SlowlogEntries struct {
	Cluster string          `json:"cluster"`
	Entries []*SlowlogEntry `json:"entries"`
}

// NewSlowlogEntry build empty slowlog
func NewSlowlogEntry(ctype types.CacheType) *SlowlogEntry {
	return &SlowlogEntry{
		CacheType: ctype,
		Cmd:       nil,
		StartTime: defaultTime,
		TotalDur:  time.Duration(0),
		RemoteDur: time.Duration(0),
		Subs:      nil,
	}
}

// SlowlogEntry is each slowlog item
type SlowlogEntry struct {
	Cluster   string `json:"cluster,omitempty"`
	CacheType types.CacheType
	Cmd       []string

	StartTime    time.Time
	TotalDur     time.Duration
	RemoteDur    time.Duration
	WaitWriteDur time.Duration
	PreEndDur    time.Duration
	PipeDur      time.Duration
	InputDur     time.Duration
	Addr         string
	Subs         []*SlowlogEntry `json:"Subs,omitempty"`
}

// collapseSymbol is the fill in strings.
var collapseSymbol = []byte("...")

// CollapseBody will copy the src data if src is small than 256 byte
// otherwise it will copy a collapsed body only contains the beginning bytes
// with length 256 - len("...") = 253
func CollapseBody(src []byte) (dst []byte) {
	if len(src) < copyCollapsedMaxLength {
		dst = make([]byte, len(src))
		copy(dst, src)
		return
	}

	dst = make([]byte, 256)
	copy(dst, src[:copyCollapsedMaxLength])
	copy(dst[copyCollapsedMaxLength:], collapseSymbol)
	return
}
