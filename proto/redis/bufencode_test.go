package redis

import (
	"bytes"
	"testing"

	"io"

	"github.com/stretchr/testify/assert"
)

func _buildBufMut() (*bytes.Buffer, *buffer) {
	var buf = new(bytes.Buffer)
	var cpy = new(bytes.Buffer)
	writer := io.MultiWriter(buf, cpy)

	return cpy, newBuffer(nil, writer)
}

func _readAll(t *testing.T, buf *bytes.Buffer) []byte {
	checked := make([]byte, 1024)
	size, err := buf.Read(checked)
	assert.NoError(t, err)
	return checked[:size]
}

func TestEncodeRespPlainOk(t *testing.T) {
	cpy, en := _buildBufMut()
	resp := newRespPlain(respString, []byte("Bilibili 干杯 - ( ゜- ゜)つロ"))
	err := en.encodeRespPlain(resp)
	assert.NoError(t, err, "fail to encode RespPlain")
	assert.NoError(t, en.bw.Flush())

	buf := _readAll(t, cpy)
	assert.Equal(t, "+Bilibili 干杯 - ( ゜- ゜)つロ\r\n", string(buf))
}

func TestEncodeRespBulkNullOk(t *testing.T) {
	cpy, en := _buildBufMut()
	resp := newRespPlain(respBulk, nil)
	err := en.encodeRespBulk(resp)
	assert.NoError(t, err, "fail to encode RespPlain")
	assert.NoError(t, en.bw.Flush())

	buf := _readAll(t, cpy)
	assert.Equal(t, "$-1\r\n", string(buf))
}

func TestEncodeRespBulkNotNullOk(t *testing.T) {
	cpy, en := _buildBufMut()
	resp := newRespPlain(respBulk, []byte("Bilibili 干杯 - ( ゜- ゜)つロ"))
	err := en.encodeRespBulk(resp)
	assert.NoError(t, err, "fail to encode resp bulk")
	assert.NoError(t, en.bw.Flush())

	buf := _readAll(t, cpy)
	assert.Equal(t, "$35\r\nBilibili 干杯 - ( ゜- ゜)つロ\r\n", string(buf))
}

func TestEncodeRespBulkEmptyOk(t *testing.T) {
	cpy, en := _buildBufMut()
	resp := newRespPlain(respBulk, []byte(""))
	err := en.encodeRespBulk(resp)
	assert.NoError(t, err, "fail to encode resp bulk")
	assert.NoError(t, en.bw.Flush())

	buf := _readAll(t, cpy)
	assert.Equal(t, "$0\r\n\r\n", string(buf))
}

func TestEncodeRespArrayNullOk(t *testing.T) {
	cpy, en := _buildBufMut()
	resp := newRespPlain(respArray, nil)
	err := en.encodeRespArray(resp)
	assert.NoError(t, err, "fail to encode resp bulk")
	assert.NoError(t, en.bw.Flush())

	buf := _readAll(t, cpy)
	assert.Equal(t, "*-1\r\n", string(buf))
}

func TestEncodeRespArrayOk(t *testing.T) {
	cpy, en := _buildBufMut()
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
	cpy, en := _buildBufMut()
	resp := newRespArray([]*resp{})

	err := en.encodeRespArray(resp)
	assert.NoError(t, err, "fail to encode resp bulk")
	assert.NoError(t, en.bw.Flush())

	buf := _readAll(t, cpy)
	assert.Equal(t, "*0\r\n", string(buf))
}
