package redis

import (
	"testing"

	"github.com/felixhao/overlord/proto"
	"github.com/stretchr/testify/assert"
)

func TestEncodeCacheTypeMissMatch(t *testing.T) {
	response := &proto.Response{Type: proto.CacheTypeMemcache}
	_, buf := _buildBufMut()
	en := &encoder{buf}
	err := en.Encode(response)
	assert.Error(t, err)
	assert.Equal(t, ErrMissMatchResponseType, err)
}

func TestEncodeResponseOk(t *testing.T) {
	response := &proto.Response{Type: proto.CacheTypeRedis}
	proto := newRResponse(MergeTypeBasic, nil)
	proto.respObj = newRespArray([]*resp{
		newRespPlain(respString, []byte("get")),
		newRespBulk([]byte("my")),
		newRespInt(1024),
	})
	response.WithProto(proto)
	cpy, buf := _buildBufMut()
	en := &encoder{buf}
	err := en.Encode(response)
	assert.NoError(t, err)

	data := _readAll(t, cpy)
	assert.Equal(t, "*3\r\n+get\r\n$2\r\nmy\r\n:1024\r\n", string(data))
}

func TestDecodeRequestOk(t *testing.T) {
	buf := _buildBuffer([]byte("*3\r\n+get\r\n$2\r\nmy\r\n:1024\r\n"))
	de := &decoder{buf: buf}

	req, err := de.Decode()
	assert.NoError(t, err)
	proto, ok := req.Proto().(*RRequest)
	assert.True(t, ok)
	assert.Len(t, proto.respObj.array, 3)
	assert.Equal(t, "get", string(proto.respObj.nth(0).data))
	assert.Equal(t, "my", string(proto.respObj.nth(1).data))
	assert.Equal(t, "1024", string(proto.respObj.nth(2).data))
}
