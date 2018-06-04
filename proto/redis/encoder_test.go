package redis

import (
	"bytes"
	"testing"

	"bufio"

	"io"

	"github.com/felixhao/overlord/proto"
	"github.com/stretchr/testify/assert"
)

func _buildEncoder() (*bytes.Buffer, *encoder) {
	var buf = new(bytes.Buffer)
	var cpy = new(bytes.Buffer)
	writer := io.MultiWriter(buf, cpy)
	return cpy, &encoder{bw: bufio.NewWriter(writer)}
}

func _readAll(t *testing.T, buf *bytes.Buffer) []byte {
	checked := make([]byte, 1024)
	size, err := buf.Read(checked)
	assert.NoError(t, err)
	return checked[:size]
}

func TestEncodeRespPlainOk(t *testing.T) {
	cpy, en := _buildEncoder()
	resp := newRespPlain(respString, []byte("Bilibili 干杯 - ( ゜- ゜)つロ"))
	err := en.encodeRespPlain(resp)
	assert.NoError(t, err, "fail to encode RespPlain")
	assert.NoError(t, en.bw.Flush())

	buf := _readAll(t, cpy)
	assert.Equal(t, "+Bilibili 干杯 - ( ゜- ゜)つロ\r\n", string(buf))
}

func TestEncodeRespBulkNullOk(t *testing.T) {
	cpy, en := _buildEncoder()
	resp := newRespPlain(respBulk, nil)
	err := en.encodeRespBulk(resp)
	assert.NoError(t, err, "fail to encode RespPlain")
	assert.NoError(t, en.bw.Flush())

	buf := _readAll(t, cpy)
	assert.Equal(t, "$-1\r\n", string(buf))
}

func TestEncodeRespBulkNotNullOk(t *testing.T) {
	cpy, en := _buildEncoder()
	resp := newRespPlain(respBulk, []byte("Bilibili 干杯 - ( ゜- ゜)つロ"))
	err := en.encodeRespBulk(resp)
	assert.NoError(t, err, "fail to encode resp bulk")
	assert.NoError(t, en.bw.Flush())

	buf := _readAll(t, cpy)
	assert.Equal(t, "$35\r\nBilibili 干杯 - ( ゜- ゜)つロ\r\n", string(buf))
}

func TestEncodeRespBulkEmptyOk(t *testing.T) {
	cpy, en := _buildEncoder()
	resp := newRespPlain(respBulk, []byte(""))
	err := en.encodeRespBulk(resp)
	assert.NoError(t, err, "fail to encode resp bulk")
	assert.NoError(t, en.bw.Flush())

	buf := _readAll(t, cpy)
	assert.Equal(t, "$0\r\n\r\n", string(buf))
}

func TestEncodeRespArrayNullOk(t *testing.T) {
	cpy, en := _buildEncoder()
	resp := newRespPlain(respArray, nil)
	err := en.encodeRespArray(resp)
	assert.NoError(t, err, "fail to encode resp bulk")
	assert.NoError(t, en.bw.Flush())

	buf := _readAll(t, cpy)
	assert.Equal(t, "*-1\r\n", string(buf))
}

func TestEncodeRespArrayOk(t *testing.T) {
	cpy, en := _buildEncoder()
	resp := newRespArray([]*resp{
		newRespPlain(respString, []byte("get")),
		newRespBulk([]byte("my")),
		newRespInt(1024),
	})
	err := en.encodeRespArray(resp)
	assert.NoError(t, err, "fail to encode resp bulk")
	assert.NoError(t, en.bw.Flush())

	buf := _readAll(t, cpy)
	assert.Equal(t, "*3\r\n+get\r\n$2\r\nmy\r\n:1024\r\n", string(buf))
}

func TestEncodeRespArrayEmptyOk(t *testing.T) {
	cpy, en := _buildEncoder()
	resp := newRespArray([]*resp{})

	err := en.encodeRespArray(resp)
	assert.NoError(t, err, "fail to encode resp bulk")
	assert.NoError(t, en.bw.Flush())

	buf := _readAll(t, cpy)
	assert.Equal(t, "*0\r\n", string(buf))
}

func TestEncodeCacheTypeMissMatch(t *testing.T) {
	response := &proto.Response{Type: proto.CacheTypeMemcache}
	_, en := _buildEncoder()
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
	cpy, en := _buildEncoder()
	err := en.Encode(response)
	assert.NoError(t, err)

	buf := _readAll(t, cpy)
	assert.Equal(t, "*3\r\n+get\r\n$2\r\nmy\r\n:1024\r\n", string(buf))
}
