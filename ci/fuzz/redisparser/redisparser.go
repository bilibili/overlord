package redisparser

import (
	"bytes"
	"io"
	"net"
	"sync/atomic"
	"time"

	"overlord/pkg/bufio"
	libnet "overlord/pkg/net"
	"overlord/proxy/proto"
	"overlord/proxy/proto/redis"
)

var (
	pc   proto.ProxyConn
	msgs []*proto.Message
	nc   *libnet.Conn
)

func Fuzz(data []byte) int {
	conn := _createConn(data)
	nc := libnet.NewConn(conn, time.Second, time.Second)
	pc = redis.NewProxyConn(nc, true)
	msgs = proto.GetMsgs(4)
	nmsgs, err := pc.Decode(msgs)
	if err == bufio.ErrBufferFull {
		return -1
	}

	if err != nil {
		return 1
	}

	if len(nmsgs) >= 1 {
		return 1
	}

	return 0
}

const (
	stateClosed  = 1
	stateOpening = 0
)

type mockAddr string

func (m mockAddr) Network() string {
	return "tcp"
}
func (m mockAddr) String() string {
	return string(m)
}

type mockConn struct {
	addr   mockAddr
	buf    *bytes.Buffer
	err    error
	closed int32
}

func (m *mockConn) Read(b []byte) (n int, err error) {
	if atomic.LoadInt32(&m.closed) == stateClosed {
		return 0, io.EOF
	}
	if m.err != nil {
		err = m.err
		return
	}
	return m.buf.Read(b)
}

func (m *mockConn) Write(b []byte) (n int, err error) {
	if atomic.LoadInt32(&m.closed) == stateClosed {
		return 0, io.EOF
	}

	if m.err != nil {
		err = m.err
		return
	}
	return m.buf.Write(b)
}

// writeBuffers impl the net.buffersWriter to support writev
func (m *mockConn) writeBuffers(buf *net.Buffers) (int64, error) {
	if m.err != nil {
		return 0, m.err
	}
	return buf.WriteTo(m.buf)
}

func (m *mockConn) Close() error {
	atomic.StoreInt32(&m.closed, stateClosed)
	return nil
}

func (m *mockConn) LocalAddr() net.Addr  { return m.addr }
func (m *mockConn) RemoteAddr() net.Addr { return m.addr }

func (m *mockConn) SetDeadline(t time.Time) error      { return nil }
func (m *mockConn) SetReadDeadline(t time.Time) error  { return nil }
func (m *mockConn) SetWriteDeadline(t time.Time) error { return nil }

// _createConn is useful tools for handler test
func _createConn(data []byte) net.Conn {
	mconn := &mockConn{
		addr: "127.0.0.1:12345",
		buf:  bytes.NewBuffer(data),
	}
	return mconn
}
