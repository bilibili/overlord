package proto

import (
	"fmt"
	"overlord/pkg/types"
	"strconv"
	"strings"
	"time"
)

const copyCollapsedMaxLength = 256 - 3

// SlowlogEntries is the struct with additional infomation of slowlog
type SlowlogEntries struct {
	Cluster string
	Entries []*SlowlogEntry
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
	CacheType types.CacheType
	Cmd       [][]byte

	StartTime time.Time
	TotalDur  time.Duration
	RemoteDur time.Duration
	Subs      []*SlowlogEntry
}

func (s *SlowlogEntry) String() string {
	var sb strings.Builder
	if len(s.Subs) == 0 {
		_, _ = sb.WriteString(fmt.Sprintf("cache_type=%s start_time=%d total_duration=%dus remote_duration=%dus cmd=[", s.CacheType, s.StartTime.UnixNano()/1000000000, s.TotalDur/time.Microsecond, s.RemoteDur/time.Microsecond))
		tmps := make([]string, len(s.Cmd))
		for i, cmd := range s.Cmd {
			tmps[i] = strconv.Quote(string(cmd))
		}
		_, _ = sb.WriteString(strings.Join(tmps, ", "))
		sb.WriteString("]")
	} else {
		for _, sub := range s.Subs {
			sb.WriteString("batch slowlog \"")
			sb.WriteString(sub.String())
			sb.WriteString("\"\n")
		}
	}
	return sb.String()
}

// collapseSymbol is the fill in strings.
var collapseSymbol = []byte("...")

// CollapseBody will copy the src data if src is small than 256 byte
// otherwise it will copy a collapsed body only contains the beginning bytes
// with length 256 - len("...") = 253
func CollapseBody(src []byte) (dst []byte) {
	if len(src) < copyCollapsedMaxLength {
		dst = make([]byte, len(src), len(src))
		copy(dst, src)
		return
	}

	dst = make([]byte, 256, 256)
	copy(dst, src[:copyCollapsedMaxLength])
	copy(dst[copyCollapsedMaxLength:], collapseSymbol)
	return
}
