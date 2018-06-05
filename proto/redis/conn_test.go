package redis

import (
	"bytes"
	"net"
	"testing"
	"time"

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

func (m *mockConn) Close() error         { return nil }
func (m *mockConn) LocalAddr() net.Addr  { return m.addr }
func (m *mockConn) RemoteAddr() net.Addr { return m.addr }

func (m *mockConn) SetDeadline(t time.Time) error      { return nil }
func (m *mockConn) SetReadDeadline(t time.Time) error  { return nil }
func (m *mockConn) SetWriteDeadline(t time.Time) error { return nil }

// _createConn is useful tools for handler test
func _createConn(data []byte) *connection {
	mconn := &mockConn{
		addr: "127.0.0.1:12345",
		rbuf: bytes.NewBuffer(data),
		wbuf: new(bytes.Buffer),
	}
	conn := newConn(mconn, time.Second, time.Second)
	return conn
}

func TestConnectionProxyImplConnAlwaysCallMock(t *testing.T) {
	mconn := &mockConn{addr: "127.0.0.1:12345"}
	conn := newConn(mconn, time.Second, time.Second)
	assert.Nil(t, conn.SetDeadline(time.Now()))
	assert.Nil(t, conn.SetReadDeadline(time.Now()))
	assert.Nil(t, conn.SetWriteDeadline(time.Now()))
	assert.Nil(t, conn.Close())
	assert.Equal(t, conn.LocalAddr(), conn.RemoteAddr())

	// due mock conn: we can't test for type assertion of TcpConn
	assert.NoError(t, conn.CloseReader())
}

func _testReadWriteWithMockTimeout(t *testing.T, rt, wt time.Duration) {
	data := []byte("Bilibili 干杯 - ( ゜- ゜)つロ")
	mconn := &mockConn{
		rbuf: bytes.NewBuffer(data),
		wbuf: new(bytes.Buffer),
	}
	conn := newConn(mconn, rt, wt)

	recv := make([]byte, len(data))

	size, err := conn.Read(recv)
	assert.NoError(t, err)
	assert.Equal(t, len(data), size)
	assert.Equal(t, data, recv)

	size, err = conn.Write(data)
	assert.NoError(t, err)
	assert.Equal(t, len(data), size)

}

func TestConnectionProxyMockReadWriteOk(t *testing.T) {
	_testReadWriteWithMockTimeout(t, time.Second, time.Second)
}

func TestConnectionProxyMockReadWriteZero(t *testing.T) {
	_testReadWriteWithMockTimeout(t, 0, 0)
}

func TestConnectionResetReadWriteTimeout(t *testing.T) {
	data := []byte("Bilibili 干杯 - ( ゜- ゜)つロ")
	mconn := &mockConn{
		rbuf: bytes.NewBuffer(data),
		wbuf: new(bytes.Buffer),
	}
	conn := newConn(mconn, time.Second, time.Second)

	recv := make([]byte, len(data))
	_, err := conn.Read(recv)
	assert.NoError(t, err)
	_, err = conn.Write(data)
	assert.NoError(t, err)

	// TODO(wayslog): reexport change attributes as a function
	conn.readerTimeout = 0
	conn.writerTimeout = 0
	mconn = &mockConn{
		rbuf: bytes.NewBuffer(data),
		wbuf: new(bytes.Buffer),
	}
	conn.sock = mconn

	recv = make([]byte, len(data))
	_, err = conn.Read(recv)
	assert.NoError(t, err)
	_, err = conn.Write(data)
	assert.NoError(t, err)
}
