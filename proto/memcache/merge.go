package memcache

import (
	"bytes"
	"overlord/proto"
)

// merger is the merge caller, but it never store any state.
type merger struct{}

// NewMerger will create new mc merger
func NewMerger() proto.Merger {
	return &merger{}
}

// Merge the response and set the response into subResps
func (r *merger) Merge(m *proto.Message) error {
	mcr, ok := m.Request().(*MCRequest)
	if !ok {
		m.AddSubResps(serverErrorPrefixBytes, []byte(ErrAssertMsg.Error()), crlfBytes)
		return nil
	}

	_, withValue := withValueTypes[mcr.rTp]
	trimEnd := withValue && m.IsBatch()
	for _, sub := range m.Subs() {
		submcr := sub.Request().(*MCRequest)
		bs := submcr.data
		if trimEnd {
			bs = bytes.TrimSuffix(submcr.data, endBytes)
		}
		if len(bs) == 0 {
			continue
		}
		m.AddSubResps(bs)
	}
	if trimEnd {
		m.AddSubResps(endBytes)
	}
	return nil
}
