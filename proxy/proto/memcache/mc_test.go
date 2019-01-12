package memcache

import (
	"bytes"
	"net"
	"time"

	libnet "overlord/pkg/net"
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
	rbuf   *bytes.Buffer
	wbuf   *bytes.Buffer
	data   []byte
	repeat int
	err    error
}

func (m *mockConn) Read(b []byte) (n int, err error) {
	if m.err != nil {
		err = m.err
		return
	}
	if m.repeat > 0 {
		m.rbuf.Write(m.data)
		m.repeat--
	}
	return m.rbuf.Read(b)
}
func (m *mockConn) Write(b []byte) (n int, err error) {
	if m.err != nil {
		err = m.err
		return
	}
	return m.wbuf.Write(b)
}

// writeBuffers impl the net.buffersWriter to support writev
func (m *mockConn) writeBuffers(buf *net.Buffers) (int64, error) {
	if m.err != nil {
		return 0, m.err
	}
	return buf.WriteTo(m.wbuf)
}

func (m *mockConn) Close() error         { return nil }
func (m *mockConn) LocalAddr() net.Addr  { return m.addr }
func (m *mockConn) RemoteAddr() net.Addr { return m.addr }

func (m *mockConn) SetDeadline(t time.Time) error      { return nil }
func (m *mockConn) SetReadDeadline(t time.Time) error  { return nil }
func (m *mockConn) SetWriteDeadline(t time.Time) error { return nil }

// _createConn is useful tools for handler test
func _createConn(data []byte) *libnet.Conn {
	return _createRepeatConn(data, 1)
}

func _createRepeatConn(data []byte, r int) *libnet.Conn {
	mconn := &mockConn{
		addr:   "127.0.0.1:12345",
		rbuf:   bytes.NewBuffer(nil),
		wbuf:   new(bytes.Buffer),
		data:   data,
		repeat: r,
	}
	conn := libnet.NewConn(mconn, time.Second, time.Second)
	return conn
}
