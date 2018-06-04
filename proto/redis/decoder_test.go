package redis

import (
	"bufio"
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

func _buildDecoder(data []byte) *decoder {
	buf := bytes.NewBuffer(data)
	return &decoder{br: bufio.NewReader(buf)}
}

func TestDecodeErrByRespNotSupportRESPType(t *testing.T) {
	de := _buildDecoder([]byte("@bcdefg\r\n"))
	_, err := de.decodeRespObj()
	assert.Error(t, err)
	assert.Equal(t, ErrNotSupportRESPType, err)
}

func TestDecodeRespPlainStringOk(t *testing.T) {
	de := _buildDecoder([]byte("+bug\r\n"))
	robj, err := de.decodeRespPlain()
	assert.NoError(t, err)
	assert.Equal(t, respString, robj.rtype)
	assert.Equal(t, []byte("bug"), robj.data)
}

func TestDecodeRespPlainErrorOk(t *testing.T) {
	de := _buildDecoder([]byte("-error by the boy next door\r\n"))
	robj, err := de.decodeRespPlain()
	assert.NoError(t, err)
	assert.Equal(t, respError, robj.rtype)
	assert.Equal(t, []byte("error by the boy next door"), robj.data)
}

func TestDecodeRespPlainIntPositiveOk(t *testing.T) {
	de := _buildDecoder([]byte(":1024\r\n"))
	robj, err := de.decodeRespPlain()
	assert.NoError(t, err)
	assert.Equal(t, respInt, robj.rtype)
	assert.Equal(t, []byte("1024"), robj.data)
}

func TestDecodeRespPlainIntNegativeOk(t *testing.T) {
	de := _buildDecoder([]byte(":-10\r\n"))
	robj, err := de.decodeRespPlain()
	assert.NoError(t, err)
	assert.Equal(t, respInt, robj.rtype)
	assert.Equal(t, []byte("-10"), robj.data)
}

func TestDecodeRespBulkNullOk(t *testing.T) {
	de := _buildDecoder([]byte("$-1\r\n"))
	robj, err := de.decodeRespBulk()
	assert.NoError(t, err)
	assert.Equal(t, respBulk, robj.rtype)
	assert.Equal(t, []byte(nil), robj.data)
	assert.True(t, robj.isNull())

	de = _buildDecoder([]byte("$-1\r\n$-1\r\n"))
	_, err = de.decodeRespBulk()
	assert.NoError(t, err)

	robj, err = de.decodeRespBulk()
	assert.NoError(t, err)
	assert.Equal(t, respBulk, robj.rtype)
	assert.Equal(t, []byte(nil), robj.data)
	assert.True(t, robj.isNull())
}

func TestDecodeRespBulkOk(t *testing.T) {
	de := _buildDecoder([]byte("$10\r\nhelloworld\r\n"))
	robj, err := de.decodeRespBulk()
	assert.NoError(t, err)
	assert.Equal(t, respBulk, robj.rtype)
	assert.Equal(t, []byte("helloworld"), robj.data)
	assert.False(t, robj.isNull())
}

func TestDecodeRespBulkEmptyOk(t *testing.T) {
	de := _buildDecoder([]byte("$0\r\n\r\n"))
	robj, err := de.decodeRespBulk()
	assert.NoError(t, err)
	assert.Equal(t, respBulk, robj.rtype)
	assert.Equal(t, []byte(""), robj.data)
	assert.False(t, robj.isNull())
}

func TestDecodeRespArrayNullOk(t *testing.T) {
	de := _buildDecoder([]byte("*-1\r\n"))
	robj, err := de.decodeRespArray()
	assert.NoError(t, err)
	assert.Equal(t, respArray, robj.rtype)
	assert.Equal(t, ([]byte)(nil), robj.data)
	assert.Equal(t, ([]*resp)(nil), robj.array)
	assert.True(t, robj.isNull())
}

func TestDecodeRespArrayEmptyOk(t *testing.T) {
	de := _buildDecoder([]byte("*0\r\n"))
	robj, err := de.decodeRespArray()
	assert.NoError(t, err)
	assert.Equal(t, respArray, robj.rtype)
	assert.Len(t, robj.array, 0)
	assert.False(t, robj.isNull())
}

func TestDecodeRespArrayOk(t *testing.T) {
	de := _buildDecoder([]byte("*3\r\n+get\r\n$2\r\nmy\r\n:1024\r\n"))
	robj, err := de.decodeRespArray()
	assert.NoError(t, err)
	assert.Equal(t, respArray, robj.rtype)
	assert.Len(t, robj.array, 3)
	assert.False(t, robj.isNull())

	assert.Equal(t, "get", string(robj.nth(0).data))
	assert.Equal(t, respString, robj.nth(0).rtype)

	assert.Equal(t, "my", string(robj.nth(1).data))
	assert.Equal(t, respBulk, robj.nth(1).rtype)

	assert.Equal(t, "1024", string(robj.nth(2).data))
	assert.Equal(t, respInt, robj.nth(2).rtype)
}
