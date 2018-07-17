package memcache

import (
	"bytes"
	"overlord/proto"
)

// Merge the response and set the response into subResps
func (*proxyConn) Merge(m *proto.Message) error {
	mcr, ok := m.Request().(*MCRequest)
	if !ok {
		m.AddSubResps(serverErrorPrefixBytes, []byte(ErrAssertReq.Error()), crlfBytes)
		return nil
	}
	if !m.IsBatch() {
		m.AddSubResps(mcr.data)
		return nil
	}

	_, withValue := withValueTypes[mcr.rTp]
	trimEnd := withValue
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
