package redis

import (
	"overlord/proto"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDecodeBasicOk(t *testing.T) {
	msgs := proto.GetMsgSlice(16)
	data := "*2\r\n$3\r\nGET\r\n$4\r\nbaka\r\n"
	conn := _createConn([]byte(data))
	pc := NewProxyConn(conn)

	nmsgs, err := pc.Decode(msgs)
	assert.NoError(t, err)
	assert.Len(t, nmsgs, 1)
}

func TestDecodeComplexOk(t *testing.T) {
	msgs := proto.GetMsgSlice(16)
	data := "*3\r\n$4\r\nMGET\r\n$4\r\nbaka\r\n$4\r\nkaba\r\n"
	conn := _createConn([]byte(data))
	pc := NewProxyConn(conn)

	nmsgs, err := pc.Decode(msgs)
	assert.NoError(t, err)
	assert.Len(t, nmsgs, 1)
	assert.Len(t, nmsgs[0].Batch(), 2)
}
