package memcache

import (
	"bytes"
	"overlord/proto"
)

// Merge the response and set the response into subResps
func (p *proxyConn) Merge(m *proto.Message) error {
	mcr, ok := m.Request().(*MCRequest)
	if !ok {
		p.bw.Write(serverErrorPrefixBytes)
		p.bw.Write([]byte(ErrAssertReq.Error()))
		p.bw.Write(crlfBytes)
		// m.AddSubResps(serverErrorPrefixBytes, []byte(ErrAssertReq.Error()), crlfBytes)
		return nil
	}
	if !m.IsBatch() {
		p.bw.Write(mcr.data)
		// m.AddSubResps(mcr.data)
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
		p.bw.Write(bs)
		// m.AddSubResps(bs)
	}
	if trimEnd {
		p.bw.Write(endBytes)
		// m.AddSubResps(endBytes)
	}
	return nil
}
