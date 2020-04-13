package memcache

import (
	"fmt"
	"net"
	"testing"
	"time"

	"overlord/pkg/bufio"
	"overlord/pkg/mockconn"
	libnet "overlord/pkg/net"
	"overlord/proxy/proto"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

func _createNodeConn(data []byte) *nodeConn {
	conn := libnet.NewConn(mockconn.CreateConn(data, 1), time.Second, time.Second)
	nc := &nodeConn{
		cluster: "clusterA",
		addr:    "127.0.0.1:5000",
		bw:      bufio.NewWriter(conn),
		br:      bufio.NewReader(conn, bufio.Get(1024)),
		conn:    conn,
	}
	return nc
}

func _createReqMsg(rtype RequestType, key, data []byte) *proto.Message {
	mc := &MCRequest{
		respType: rtype,
		key:      key,
		data:     data,
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
			msg := _createReqMsg(tt.rtype, []byte(tt.key), []byte(tt.data))
			nc := _createNodeConn(nil)

			err := nc.Write(msg)
			assert.NoError(t, err)
			err = nc.Flush()
			assert.NoError(t, err)

			m, ok := nc.conn.Conn.(*mockconn.MockConn)
			assert.True(t, ok)

			buf := make([]byte, 1024)
			size, err := m.Wbuf.Read(buf)
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
		t.Run(fmt.Sprintf("%v ok", tt.rtype), func(t *testing.T) {
			msg := _createReqMsg(tt.rtype, []byte(tt.key), []byte(tt.data))
			nc := _createNodeConn(nil)

			err := nc.Write(msg)
			assert.NoError(t, err)
			err = nc.Flush()
			assert.NoError(t, err)

			c, ok := nc.conn.Conn.(*mockconn.MockConn)
			assert.True(t, ok)

			buf := make([]byte, 1024)
			size, err := c.Wbuf.Read(buf)
			assert.NoError(t, err)
			assert.Equal(t, tt.except, string(buf[:size]))
		})

	}
}

func TestNodeConnWriteBatchHasErr(t *testing.T) {
	msg := _createReqMsg(RequestTypeGet, []byte("mykey"), []byte("\r\n"))
	nc := _createNodeConn(nil)
	ec := libnet.NewConn(mockconn.CreateConn(nil, 1), time.Second, time.Second)
	ec.Conn.(*mockconn.MockConn).Err = errors.New("write error")
	nc.bw = bufio.NewWriter(ec)
	nc.bw.Write([]byte("err"))
	nc.bw.Flush() // action err

	err := nc.Write(msg)
	assert.EqualError(t, errors.Cause(err), "write error")
}

func TestNodeConnWriteBatchFlushHasErr(t *testing.T) {
	msg := _createReqMsg(RequestTypeGet, []byte("mykey"), []byte("\r\n"))
	nc := _createNodeConn(nil)
	ec := libnet.NewConn(mockconn.CreateConn(nil, 1), time.Second, time.Second)
	ec.Conn.(*mockconn.MockConn).Err = errors.New("flush error")
	nc.bw = bufio.NewWriter(ec)

	nc.Write(msg)
	err := nc.Flush()
	assert.EqualError(t, errors.Cause(err), "flush error")
}

func TestNodeConnWriteClosed(t *testing.T) {
	msg := _createReqMsg(RequestTypeGet, []byte("abc"), []byte(" \r\n"))
	nc := _createNodeConn(nil)
	err := nc.Close()
	assert.NoError(t, err)
	assert.True(t, nc.Closed())
	err = nc.Write(msg)
	assert.Error(t, err)
	_causeEqual(t, ErrClosed, err)
	assert.NoError(t, nc.Close())
	_causeEqual(t, ErrClosed, nc.Write(nil))
	err = nc.Flush()
	assert.Error(t, err)
	_causeEqual(t, ErrClosed, err)
}

type mockReq struct {
}

func (r *mockReq) Merge([]proto.Request) error {
	return nil
}

func (*mockReq) Slowlog() *proto.SlowlogEntry { return nil }

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
	msg := proto.NewMessage()
	nc := _createNodeConn(nil)
	msg.WithRequest(&mockReq{})
	err := nc.Write(msg)
	assert.Error(t, err)
	_causeEqual(t, ErrAssertReq, err)
}

func TestNodeConnReadClosed(t *testing.T) {
	msg := _createReqMsg(RequestTypeGet, []byte("abc"), []byte(" \r\n"))
	nc := _createNodeConn(nil)

	err := nc.Close()
	assert.NoError(t, err)
	assert.True(t, nc.Closed())

	err = nc.Read(msg)
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
			msg := _createReqMsg(tt.rtype, []byte(tt.key), []byte(tt.data))
			nc := _createNodeConn([]byte(tt.cData))

			err := nc.Read(msg)
			assert.NoError(t, err)
			mcr, ok := msg.Request().(*MCRequest)
			assert.True(t, ok)
			assert.NotNil(t, mcr)
			assert.Equal(t, tt.except, string(mcr.data))
		})

	}
}

func TestNodeConnReadWithLargeValue(t *testing.T) {
	msg := _createReqMsg(RequestTypeGet, []byte("mykey"), []byte("\r\n"))
	bodySize := 1048576
	head := []byte("VALUE a 1 1048576\r\n")
	tail := "\r\nEND\r\n"

	data := []byte{}
	for i := 0; i < 3; i++ {
		data = append(data, head...)
		data = append(data, make([]byte, bodySize)...)
		data = append(data, tail...)
	}

	nc := _createNodeConn(data)
	for i := 0; i < 3; i++ {
		t.Run(fmt.Sprintf("times-%d", i+1), func(t *testing.T) {
			err := nc.Read(msg)
			mcr := msg.Request().(*MCRequest)
			assert.Len(t, mcr.data, len(head)+bodySize+len(tail))
			assert.NoError(t, err)
		})
	}

}

func TestNodeConnReadHasErr(t *testing.T) {
	msg := _createReqMsg(RequestTypeGet, []byte("mykey"), []byte("\r\n"))
	nc := _createNodeConn([]byte("END\r\n"))
	ec := libnet.NewConn(mockconn.CreateConn(nil, 1), time.Second, time.Second)
	ec.Conn.(*mockconn.MockConn).Err = errors.New("read error")
	nc.br = bufio.NewReader(ec, bufio.Get(128))

	err := nc.Read(msg)
	assert.EqualError(t, errors.Cause(err), "read error")
}

func TestNodeConnFindLengthErr(t *testing.T) {
	msg := _createReqMsg(RequestTypeGet, []byte("mykey"), []byte("\r\n"))
	nc := _createNodeConn([]byte("VALUE mykey 0 a\r\nEND\r\n"))

	err := nc.Read(msg)
	_causeEqual(t, ErrBadLength, errors.Cause(err))
}

func TestNodeConnReadExtraErr(t *testing.T) {
	msg := _createReqMsg(RequestTypeGet, []byte("mykey"), []byte("\r\n"))
	nc := _createNodeConn([]byte("VALUE mykey 0 3\r\n"))

	err := nc.Read(msg)
	assert.EqualError(t, errors.Cause(err), "EOF")
}

func TestNodeConnReadBufFull(t *testing.T) {
	msg := _createReqMsg(RequestTypeGet, []byte("mykey"), []byte("\r\n"))
	nc := _createNodeConn([]byte("END"))

	err := nc.Read(msg) // NOTE: because "END" no "\r\n", so BufferFull first and break, then EOF
	assert.EqualError(t, errors.Cause(err), "EOF")
}

func TestNodeConnAssertError(t *testing.T) {
	nc := _createNodeConn(nil)
	msg := proto.NewMessage()
	msg.WithRequest(&mockReq{})

	err := nc.Read(msg)
	_causeEqual(t, ErrAssertReq, errors.Cause(err))
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
