package mcNodeConnRetrival

import (
	"overlord/proxy/proto"
	"overlord/proxy/proto/memcache"
)

func Fuzz(data []byte) int {
	msg := proto.GetMsgs(1, 1)[0]
	nc := memcache.NewNodeConnWithLibConn("test-mc", "127.0.0.1", _createLibConn(data))

	memcache.WithReq(msg, memcache.RequestTypeGet, []byte("1824"), []byte("\r\n"))
	if err := nc.Read(msg); err != nil {
		return 1
	}
	return 0
}
