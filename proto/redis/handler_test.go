package redis

import (
	"testing"

	"github.com/felixhao/overlord/proto"
	"github.com/stretchr/testify/assert"
)

func TestHandlerHandleClosed(t *testing.T) {
	respData := []byte("+OK\r\n")
	conn := _createConn(respData)
	hd := handler{
		cluster: "mycluster",
		addr:    "127.0.0.1:7001",
		conn:    conn,
		buf:     newBuffer(conn, conn),
		closed:  handlerClosed,
	}

	response, err := hd.Handle(&proto.Request{Type: proto.CacheTypeRedis})
	assert.Error(t, err)
	assert.Equal(t, ErrReuseClosedConn, err)
	assert.Nil(t, response)
}

func TestHandlerHandleOk(t *testing.T) {
	respData := []byte("+OK\r\n")
	conn := _createConn(respData)
	hd := handler{
		cluster: "mycluster",
		addr:    "127.0.0.1:7001",
		conn:    conn,
		buf:     newBuffer(conn, conn),
		closed:  handlerOpening,
	}

	req := &proto.Request{Type: proto.CacheTypeRedis}
	req.WithProto(NewCommand("set", "a", "b"))
	response, err := hd.Handle(req)
	assert.NoError(t, err)
	protoResp, ok := response.Proto().(*RResponse)
	assert.True(t, ok)
	assert.Equal(t, respString, protoResp.respObj.rtype)
	assert.Equal(t, "OK", string(protoResp.respObj.data))
}
