package memcache

import (
	"fmt"
	"io"
	"testing"
	"time"

	"net"

	"overlord/lib/bufio"
	"overlord/proto"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

func _createNodeConn(data []byte) *nodeConn {
	conn := _createConn(data)
	nc := &nodeConn{
		cluster: "clusterA",
		addr:    "127.0.0.1:5000",
		bw:      bufio.NewWriter(conn),
		br:      bufio.NewReader(conn, nil),
		pinger:  newMCPinger(conn),
		conn:    conn,
	}
	return nc
}

func _createReqMsg(rtype RequestType, key, data []byte) *proto.Message {
	mc := &MCRequest{
		rTp:  rtype,
		key:  key,
		data: data,
	}
	pm := proto.NewMessage()
	pm.WithRequest(mc)
	return pm
}

func _causeEqual(t *testing.T, except, actual error) {
	err := errors.Cause(actual)
	assert.Equal(t, except, err)
}

func TestNodeConnWriteOk(t *testing.T) {
	ts := []struct {
		rtype  RequestType
		key    string
		data   string
		except string
	}{
		{rtype: RequestTypeSet, key: "mykey", data: " 0 0 1\r\na\r\n", except: "set mykey 0 0 1\r\na\r\n"},
		{rtype: RequestTypeGat, key: "mykey", data: "1024", except: "gat 1024 mykey\r\n"},
	}

	for _, tt := range ts {
		t.Run(fmt.Sprintf("%v Ok", tt.rtype), func(subt *testing.T) {
			req := _createReqMsg(tt.rtype, []byte(tt.key), []byte(tt.data))
			nc := _createNodeConn(nil)

			err := nc.write(req)
			nc.bw.Flush()
			assert.NoError(t, err)

			m, ok := nc.conn.Conn.(*mockConn)
			assert.True(t, ok)

			buf := make([]byte, 1024)
			size, err := m.wbuf.Read(buf)
			assert.NoError(t, err)
			assert.Equal(t, tt.except, string(buf[:size]))
		})
	}
}

func TestNodeConnWriteClosed(t *testing.T) {
	req := _createReqMsg(RequestTypeGet, []byte("abc"), []byte(" \r\n"))
	nc := _createNodeConn(nil)
	err := nc.Close()
	assert.NoError(t, err)
	assert.True(t, nc.Closed())
	err = nc.write(req)
	assert.Error(t, err)
	_causeEqual(t, ErrClosed, err)
	assert.NoError(t, nc.Close())
}

type mockReq struct {
}

func (*mockReq) CmdString() string {
	return ""
}

func (*mockReq) Cmd() []byte {
	return []byte("")
}

func (*mockReq) Key() []byte {
	return []byte{}
}

func (*mockReq) Resp() []byte {
	return nil
}

func (*mockReq) Put() {

}
func TestNodeConnWriteTypeAssertFail(t *testing.T) {
	req := proto.NewMessage()
	nc := _createNodeConn(nil)
	req.WithRequest(&mockReq{})
	err := nc.write(req)
	nc.bw.Flush()
	assert.Error(t, err)
	_causeEqual(t, ErrAssertReq, err)
}

func TestNodeConnReadClosed(t *testing.T) {
	req := _createReqMsg(RequestTypeGet, []byte("abc"), []byte(" \r\n"))
	nc := _createNodeConn(nil)

	err := nc.Close()
	assert.NoError(t, err)
	assert.True(t, nc.Closed())
	batch := proto.NewMsgBatch()
	batch.AddMsg(req)
	err = nc.ReadBatch(batch)
	assert.Error(t, err)
	_causeEqual(t, ErrClosed, err)
}

func TestNodeConnReadOk(t *testing.T) {
	ts := []struct {
		suffix string
		rtype  RequestType
		key    string
		data   string
		cData  string
		except string
	}{
		{
			suffix: "404",
			rtype:  RequestTypeGet, key: "mykey", data: " \r\n",
			cData: "END\r\n", except: "END\r\n",
		},
		{
			suffix: "Ok",
			rtype:  RequestTypeGet, key: "mykey", data: " \r\n",
			cData: "VALUE mykey 0 1\r\na\r\nEND\r\n", except: "VALUE mykey 0 1\r\na\r\nEND\r\n",
		},
		{
			suffix: "Ok",
			rtype:  RequestTypeSet, key: "mykey", data: "0 0 1\r\nb\r\n",
			cData: "STORED\r\n", except: "STORED\r\n",
		},
	}
	for _, tt := range ts {

		t.Run(fmt.Sprintf("%v%s", tt.rtype, tt.suffix), func(t *testing.T) {
			req := _createReqMsg(tt.rtype, []byte(tt.key), []byte(tt.data))
			nc := _createNodeConn([]byte(tt.cData))
			batch := proto.NewMsgBatch()
			batch.AddMsg(req)
			err := nc.ReadBatch(batch)
			assert.NoError(t, err)
			assert.Equal(t, tt.except, string(req.Request().Resp()))
		})

	}
}

func TestNodeConnAssertError(t *testing.T) {
	nc := _createNodeConn(nil)
	req := proto.NewMessage()
	req.WithRequest(&mockReq{})
	batch := proto.NewMsgBatch()
	batch.AddMsg(req)
	err := nc.ReadBatch(batch)
	_causeEqual(t, ErrAssertReq, err)
}

func TestNocdConnPingOk(t *testing.T) {
	nc := _createNodeConn(pong)
	err := nc.Ping()
	assert.NoError(t, err)
	assert.NoError(t, nc.Close())
	err = nc.Ping()
	assert.Error(t, err)
	_causeEqual(t, io.EOF, err)
}

func TestNewNodeConnWithClosedBinder(t *testing.T) {
	taddr, err := net.ResolveTCPAddr("tcp", "127.0.0.1:0")
	assert.NoError(t, err)
	listener, err := net.ListenTCP("tcp", taddr)
	assert.NoError(t, err)
	addr := listener.Addr()
	go func() {
		defer listener.Close()
		sock, _ := listener.Accept()
		defer sock.Close()
	}()
	nc := NewNodeConn("anyName", addr.String(), time.Second, time.Second, time.Second)
	assert.NotNil(t, nc)
}
