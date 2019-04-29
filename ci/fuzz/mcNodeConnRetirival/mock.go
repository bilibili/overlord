package mcNodeConnRetrival

import (
	"bytes"
	"io"
	"net"
	libnet "overlord/pkg/net"
	"sync/atomic"
	"time"
)

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

func _createLibConn(data []byte) *libnet.Conn {
	mconn := _createConn(data)
	return libnet.NewConn(mconn, time.Second, time.Second)
}
