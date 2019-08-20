package cluster

import (
	"math/rand"
	"reflect"
	"testing"

	"overlord/pkg/bufio"
	"overlord/proxy/proto"
	"overlord/proxy/proto/redis"

	"bou.ke/monkey"
	"github.com/stretchr/testify/assert"
)

func TestNodeConnMaxRedirect(t *testing.T) {
	monkey.Patch(newNodeConn, func(_ *cluster, addr string) proto.NodeConn {
		return &nodeConn{
			nc: &redis.NodeConn{},
			c:  &cluster{action: make(chan struct{})},
		}
	})

	nnc := newNodeConn(nil, "")
	cnc := nnc.(*nodeConn)
	rnc := cnc.nc.(*redis.NodeConn)
	bw := &bufio.Writer{}

	msgs := proto.GetMsgs(1)
	msg := msgs[0]
	req := &redis.Request{}
	msg.WithRequest(req)
	resp := &redis.RESP{}

	// msg stub
	monkey.PatchInstanceMethod(reflect.TypeOf(msg), "Request", func(*proto.Message) proto.Request {
		return req
	})
	// nc stub
	monkey.PatchInstanceMethod(reflect.TypeOf(rnc), "Read", func(_ *redis.NodeConn, _ *proto.Message) error {
		return nil
	})
	monkey.PatchInstanceMethod(reflect.TypeOf(rnc), "Bw", func(_ *redis.NodeConn) *bufio.Writer {
		return bw
	})
	monkey.PatchInstanceMethod(reflect.TypeOf(rnc), "Close", func(_ *redis.NodeConn) error {
		return nil
	})
	// req stub
	monkey.PatchInstanceMethod(reflect.TypeOf(req), "IsSupport", func(_ *redis.Request) bool {
		return true
	})
	monkey.PatchInstanceMethod(reflect.TypeOf(req), "IsCtl", func(_ *redis.Request) bool {
		return false
	})
	monkey.PatchInstanceMethod(reflect.TypeOf(req), "Reply", func(_ *redis.Request) *redis.RESP {
		return resp
	})
	monkey.PatchInstanceMethod(reflect.TypeOf(req), "RESP", func(_ *redis.Request) *redis.RESP {
		return resp
	})
	// resp stub
	monkey.PatchInstanceMethod(reflect.TypeOf(resp), "Type", func(_ *redis.RESP) byte {
		return '-'
	})
	monkey.PatchInstanceMethod(reflect.TypeOf(resp), "Data", func(_ *redis.RESP) []byte {
		var (
			s = [][]byte{
				[]byte("ASK 1 127.0.0.1:31234"),
				[]byte("MOVED 1 127.0.0.1:31234"),
			}
		)
		return s[rand.Intn(2)]
	})
	monkey.PatchInstanceMethod(reflect.TypeOf(resp), "Encode", func(_ *redis.RESP, _ *bufio.Writer) error {
		return nil
	})
	// bw stub
	monkey.PatchInstanceMethod(reflect.TypeOf(bw), "Write", func(_ *bufio.Writer, _ []byte) error {
		return nil
	})
	monkey.PatchInstanceMethod(reflect.TypeOf(bw), "Flush", func(_ *bufio.Writer) error {
		return nil
	})

	for i := 0; i < 10; i++ {
		err := nnc.Read(msg)
		assert.NoError(t, err)
	}

	monkey.UnpatchAll()
}
