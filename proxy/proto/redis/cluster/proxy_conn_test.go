package cluster

import (
	"reflect"
	"testing"

	"overlord/pkg/bufio"
	"overlord/proxy/proto"
	"overlord/proxy/proto/redis"

	"github.com/bouk/monkey"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

func TestProxyConn(t *testing.T) {
	pc := &proxyConn{
		// c:  &cluster{},
		pc: &redis.ProxyConn{},
	}
    cls := &cluster{}
	cls.fakeNodesBytes = []byte("fake nodes bytes")
	cls.fakeSlotsBytes = []byte("fake slots bytes")
	bw := &bufio.Writer{}
	msgs := proto.GetMsgs(1)
	msg := msgs[0]
	req := &redis.Request{}
	msg.WithRequest(req)
	resp := &redis.RESP{}

	// pc stub
	monkey.PatchInstanceMethod(reflect.TypeOf(pc.pc), "Bw", func(_ *redis.ProxyConn) *bufio.Writer {
		return bw
	})
	// msg stub
	monkey.PatchInstanceMethod(reflect.TypeOf(msg), "IsBatch", func(*proto.Message) bool {
		return false
	})
	monkey.PatchInstanceMethod(reflect.TypeOf(msg), "Request", func(*proto.Message) proto.Request {
		return req
	})
	// req stub
	monkey.PatchInstanceMethod(reflect.TypeOf(req), "IsSupport", func(_ *redis.Request) bool {
		return false
	})
	monkey.PatchInstanceMethod(reflect.TypeOf(req), "IsCtl", func(_ *redis.Request) bool {
		return false
	})
	monkey.PatchInstanceMethod(reflect.TypeOf(req), "RESP", func(_ *redis.Request) *redis.RESP {
		return resp
	})
	// resp stub
	monkey.PatchInstanceMethod(reflect.TypeOf(resp), "Array", func(_ *redis.RESP) []*redis.RESP {
		return []*redis.RESP{resp, resp}
	})

	// case cmdNodesBytes
	var count int
	monkey.PatchInstanceMethod(reflect.TypeOf(resp), "Data", func(_ *redis.RESP) []byte {
		if count == 0 {
			count++
			return cmdClusterBytes
		}
		return cmdNodesBytes
	})
	// bw stub
	monkey.PatchInstanceMethod(reflect.TypeOf(bw), "Write", func(_ *bufio.Writer, bs []byte) error {
		assert.Equal(t, cls.fakeNodesBytes, bs)
		return nil
	})
	err := pc.Encode(msg, cls)
	assert.NoError(t, err)

	// case cmdSlotsBytes
	count = 0
	monkey.PatchInstanceMethod(reflect.TypeOf(resp), "Data", func(_ *redis.RESP) []byte {
		if count == 0 {
			count++
			return cmdClusterBytes
		}
		return cmdSlotsBytes
	})
	// bw stub
	monkey.PatchInstanceMethod(reflect.TypeOf(bw), "Write", func(_ *bufio.Writer, bs []byte) error {
		assert.Equal(t, cls.fakeSlotsBytes, bs)
		return nil
	})
	err = pc.Encode(msg, cls)
	assert.NoError(t, err)

	// case not support
	count = 0
	monkey.PatchInstanceMethod(reflect.TypeOf(resp), "Data", func(_ *redis.RESP) []byte {
		if count == 0 {
			count++
			return cmdClusterBytes
		}
		return []byte("")
	})
	// bw stub
	monkey.PatchInstanceMethod(reflect.TypeOf(bw), "Write", func(_ *bufio.Writer, bs []byte) error {
		assert.Equal(t, notSupportBytes, bs)
		return nil
	})
	err = pc.Encode(msg, cls)
	assert.NoError(t, err)

	// resp stub
	monkey.PatchInstanceMethod(reflect.TypeOf(resp), "Data", func(_ *redis.RESP) []byte {
		return cmdClusterBytes
	})
	monkey.PatchInstanceMethod(reflect.TypeOf(resp), "Array", func(_ *redis.RESP) []*redis.RESP {
		return []*redis.RESP{resp}
	})
	err = pc.Encode(msg, cls)
	assert.EqualError(t, errors.Cause(err), ErrInvalidArgument.Error())
}
