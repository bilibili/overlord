package memcache

import (
	"bytes"
	"io"
	"net"
	"testing"
	"time"

	libnet "github.com/felixhao/overlord/lib/net"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

type mockAddr string

func (m mockAddr) Network() string {
	return "tcp"
}
func (m mockAddr) String() string {
	return string(m)
}

type mockConn struct {
	rbuf *bytes.Buffer
	wbuf *bytes.Buffer
	addr mockAddr
}

func (m *mockConn) Read(b []byte) (n int, err error) {
	return m.rbuf.Read(b)
}
func (m *mockConn) Write(b []byte) (n int, err error) {
	return m.wbuf.Write(b)
}

// writeBuffers impl the net.buffersWriter to support writev
func (m *mockConn) writeBuffers(buf *net.Buffers) (int64, error) {
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
		addr: "127.0.0.1:12345",
		rbuf: bytes.NewBuffer(bytes.Repeat(data, r)),
		wbuf: new(bytes.Buffer),
	}
	conn := libnet.NewConn(mconn, time.Second, time.Second)
	return conn
}

func TestPingerPingOk(t *testing.T) {
	conn := _createConn(pong)
	pinger := newMCPinger(conn)

	err := pinger.Ping()
	assert.NoError(t, err)
}

func TestPingerPingEOF(t *testing.T) {
	conn := _createConn(pong)
	pinger := newMCPinger(conn)

	err := pinger.Ping()
	assert.NoError(t, err)

	err = pinger.Ping()
	assert.Error(t, err)

	err = errors.Cause(err)
	assert.Equal(t, io.EOF, err)
}

func TestPingerPing100Ok(t *testing.T) {
	conn := _createRepeatConn(pong, 100)
	pinger := newMCPinger(conn)

	for i := 0; i < 100; i++ {
		err := pinger.Ping()
		assert.NoError(t, err)
	}

	err := pinger.Ping()
	assert.Error(t, err)

	err = errors.Cause(err)
	assert.Equal(t, io.EOF, err)
}

func TestPingerClosed(t *testing.T) {
	conn := _createRepeatConn(pong, 100)
	pinger := newMCPinger(conn)
	err := pinger.Close()
	assert.NoError(t, err)

	err = pinger.Ping()
	assert.Error(t, err)
}
