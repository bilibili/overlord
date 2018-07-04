package net

import (
	"bytes"
	"io"
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

func TestConnProxyImplConnAlwaysCallMock(t *testing.T) {
	mconn := &mockConn{addr: "127.0.0.1:12345"}
	conn := NewConn(mconn, time.Second, time.Second)
	assert.Nil(t, conn.SetDeadline(time.Now()))
	assert.Nil(t, conn.SetReadDeadline(time.Now()))
	assert.Nil(t, conn.SetWriteDeadline(time.Now()))
	assert.Nil(t, conn.Close())
	assert.Equal(t, conn.LocalAddr(), conn.RemoteAddr())
}

func _testReadWriteWithMockTimeout(t *testing.T, rt, wt time.Duration) {
	data := []byte("Bilibili 干杯 - ( ゜- ゜)つロ")
	mconn := &mockConn{
		rbuf: bytes.NewBuffer(data),
		wbuf: new(bytes.Buffer),
	}
	conn := NewConn(mconn, rt, wt)

	recv := make([]byte, len(data))

	size, err := conn.Read(recv)
	assert.NoError(t, err)
	assert.Equal(t, len(data), size)
	assert.Equal(t, data, recv)

	size, err = conn.Write(data)
	assert.NoError(t, err)
	assert.Equal(t, len(data), size)

}

func TestConnProxyMockReadWriteOk(t *testing.T) {
	_testReadWriteWithMockTimeout(t, time.Second, time.Second)
}

func TestConnProxyMockReadWriteZero(t *testing.T) {
	_testReadWriteWithMockTimeout(t, 0, 0)
}

func TestConnResetReadWriteTimeout(t *testing.T) {
	data := []byte("Bilibili 干杯 - ( ゜- ゜)つロ")
	mconn := &mockConn{
		rbuf: bytes.NewBuffer(data),
		wbuf: new(bytes.Buffer),
	}
	conn := NewConn(mconn, time.Second, time.Second)

	recv := make([]byte, len(data))
	_, err := conn.Read(recv)
	assert.NoError(t, err)
	_, err = conn.Write(data)
	assert.NoError(t, err)

	// TODO(wayslog): reexport change attributes as a function
	conn.readTimeout = 0
	conn.writeTimeout = 0
	mconn = &mockConn{
		rbuf: bytes.NewBuffer(data),
		wbuf: new(bytes.Buffer),
	}
	conn.Conn = mconn

	recv = make([]byte, len(data))
	_, err = conn.Read(recv)
	assert.NoError(t, err)
	_, err = conn.Write(data)
	assert.NoError(t, err)
}

func TestConnReadReConnect(t *testing.T) {
	addr, err := net.ResolveTCPAddr("tcp", "127.0.0.1:0")
	assert.NoError(t, err)
	l, err := net.ListenTCP("tcp", addr)
	assert.NoError(t, err)
	laddr := l.Addr()
	go func() {
		sock, err := l.AcceptTCP()
		assert.NoError(t, err)
		_ = sock.CloseRead()
		sock.Close()

		sock, err = l.AcceptTCP()
		_ = sock.CloseRead()
		assert.NoError(t, err)
		sock.Close()
		_ = l.Close()
	}()

	conn := DialWithTimeout(laddr.String(), time.Second, time.Second, time.Second)
	buf := make([]byte, 1024)
	_, err = conn.Read(buf)
	assert.Error(t, err)
	assert.Equal(t, io.EOF, err)

	_, err = conn.Read(buf)
	assert.Error(t, err)

	_, err = conn.Read(buf)
	assert.Error(t, err)
}

func TestConnWriteReConnect(t *testing.T) {
	addr, err := net.ResolveTCPAddr("tcp", "127.0.0.1:0")
	assert.NoError(t, err)
	l, err := net.ListenTCP("tcp", addr)
	assert.NoError(t, err)
	laddr := l.Addr()
	go func() {
		sock, err := l.Accept()
		assert.NoError(t, err)
		_ = sock.Close()

		sock, err = l.Accept()
		assert.NoError(t, err)
		_ = sock.Close()
		_ = l.Close()
	}()

	conn := DialWithTimeout(laddr.String(), time.Second, time.Second, time.Second)
	buf := []byte("Bilibili 干杯 - ( ゜- ゜)つロ")

	_ = conn.Close()
	_, err = conn.Write(buf)
	assert.Error(t, err)
	// assert.Equal(t, io.EOF, err)
	for i := 0; i < 3; i++ {
		_, err = conn.Write(buf)
		assert.Error(t, err)
	}
}

func TestConnWriteBuffersOk(t *testing.T) {
	addr, err := net.ResolveTCPAddr("tcp", "127.0.0.1:0")
	assert.NoError(t, err)
	l, err := net.ListenTCP("tcp", addr)
	assert.NoError(t, err)
	laddr := l.Addr()
	go func() {
		defer l.Close()
		buf := make([]byte, 1024)
		for {
			sock, err := l.Accept()
			assert.NoError(t, err)
			n, err := sock.Read(buf)
			assert.NoError(t, err)
			assert.NotZero(t, n)
		}
	}()
	conn := DialWithTimeout(laddr.String(), time.Second, time.Second, time.Second)
	buffers := net.Buffers([][]byte{[]byte("baka"), []byte("qiu")})
	n, err := conn.Writev(&buffers)
	assert.NoError(t, err)
	assert.Equal(t, 7, int(n))
}
