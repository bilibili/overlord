package memcache

import (
	"fmt"
	"net"
	"testing"
	"time"

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
		br:      bufio.NewReader(conn, bufio.Get(2048)),
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

func TestNodeConnWriteBatchOk(t *testing.T) {
	ts := []struct {
		rtype  RequestType
		key    string
		data   string
		except string
	}{
		{
			rtype: RequestTypeGet, key: "mykey", data: "\r\n",
			except: "get mykey\r\n",
		},
		{
			rtype: RequestTypeSet, key: "mykey", data: " 0 0 1\r\nb\r\n",
			except: "set mykey 0 0 1\r\nb\r\n",
		},
	}
	for _, tt := range ts {
		t.Run(fmt.Sprintf("%v_ok", tt.rtype), func(t *testing.T) {
			req := _createReqMsg(tt.rtype, []byte(tt.key), []byte(tt.data))
			nc := _createNodeConn(nil)
			batch := proto.NewMsgBatch()
			batch.AddMsg(req)

			err := nc.WriteBatch(batch)
			assert.NoError(t, err)
			err = nc.Flush()
			assert.NoError(t, err)

			c, ok := nc.conn.Conn.(*mockConn)
			assert.True(t, ok)

			buf := make([]byte, 1024)
			size, err := c.wbuf.Read(buf)
			assert.NoError(t, err)
			assert.Equal(t, tt.except, string(buf[:size]))
		})

	}
}

func TestNodeConnWriteBatchHasErr(t *testing.T) {
	req := _createReqMsg(RequestTypeGet, []byte("mykey"), []byte("\r\n"))
	nc := _createNodeConn(nil)

	ec := _createConn(nil)
	ec.Conn.(*mockConn).err = errors.New("write error")
	nc.bw = bufio.NewWriter(ec)
	nc.bw.Write([]byte("err"))
	nc.bw.Flush() // action err

	batch := proto.NewMsgBatch()
	batch.AddMsg(req)

	_ = nc.WriteBatch(batch)
	err := nc.Flush()
	assert.EqualError(t, err, "MC Writer handle flush Msg bytes: write error")
}

func TestNodeConnWriteBatchFlushHasErr(t *testing.T) {
	req := _createReqMsg(RequestTypeGet, []byte("mykey"), []byte("\r\n"))
	nc := _createNodeConn(nil)

	ec := _createConn(nil)
	ec.Conn.(*mockConn).err = errors.New("flush error")
	nc.bw = bufio.NewWriter(ec)

	batch := proto.NewMsgBatch()
	batch.AddMsg(req)

	_ = nc.WriteBatch(batch)
	err := nc.Flush()
	assert.EqualError(t, errors.Cause(err), "flush error")
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
	_causeEqual(t, ErrClosed, nc.WriteBatch(nil))
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
			rtype:  RequestTypeGet, key: "mykey", data: "\r\n",
			cData: "END\r\n", except: "END\r\n",
		},
		{
			suffix: "Ok",
			rtype:  RequestTypeGet, key: "mykey", data: "\r\n",
			cData: "VALUE mykey 0 1\r\na\r\nEND\r\n", except: "VALUE mykey 0 1\r\na\r\nEND\r\n",
		},
		{
			suffix: "Ok",
			rtype:  RequestTypeSet, key: "mykey", data: " 0 0 1\r\nb\r\n",
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
			mcr, ok := req.Request().(*MCRequest)
			assert.True(t, ok)
			assert.NotNil(t, mcr)
			assert.Equal(t, tt.except, string(mcr.data))
		})

	}
}

func TestNodeConnReadHasErr(t *testing.T) {
	req := _createReqMsg(RequestTypeGet, []byte("mykey"), []byte("\r\n"))
	nc := _createNodeConn([]byte("END\r\n"))
	ec := _createConn(nil)
	ec.Conn.(*mockConn).err = errors.New("read error")
	nc.br = bufio.NewReader(ec, bufio.Get(128))

	batch := proto.NewMsgBatch()
	batch.AddMsg(req)
	err := nc.ReadBatch(batch)
	assert.EqualError(t, errors.Cause(err), "read error")
}

func TestNodeConnReadBufFull(t *testing.T) {
	req := _createReqMsg(RequestTypeGet, []byte("mykey"), []byte("\r\n"))
	nc := _createNodeConn([]byte("END"))

	batch := proto.NewMsgBatch()
	batch.AddMsg(req)
	err := nc.ReadBatch(batch) // NOTE: because "END" no "\r\n", so BufferFull first and break, then EOF
	assert.EqualError(t, errors.Cause(err), "EOF")
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
