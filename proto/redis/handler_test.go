package redis

import (
	"testing"

	"net"

	"time"

	"runtime"

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
		closed:  handlerOpening,
	}
	err := hd.Close()
	assert.NoError(t, err)
	assert.True(t, hd.Closed())
	assert.NoError(t, hd.Close())

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

func _acceptClose(t *testing.T, fd net.Listener) {
	sock, err := fd.Accept()
	assert.NoError(t, err)
	defer sock.Close()
}

func TestHandlerDialSockWithRandomPort(t *testing.T) {
	fd, err := net.Listen("tcp", "0.0.0.0:0")
	assert.NoError(t, err)
	addr := fd.Addr()
	go _acceptClose(t, fd)

	// force to bind
	runtime.Gosched()

	dialFunc := Dial("mycluster", addr.String(), time.Second, time.Second, time.Second)
	poolConn, err := dialFunc()
	assert.NoError(t, err)

	hd, ok := poolConn.(*handler)
	assert.True(t, ok)

	_, srcport, err := net.SplitHostPort(addr.String())
	assert.NoError(t, err)
	_, destport, err := net.SplitHostPort(hd.conn.RemoteAddr())
	assert.NoError(t, err)

	assert.Equal(t, srcport, destport)
}
