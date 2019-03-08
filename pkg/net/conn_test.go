package net

import (
	"net"
	"testing"
	"time"

	"overlord/pkg/mockconn"

	"github.com/stretchr/testify/assert"
)

func TestConnProxyImplConnAlwaysCallMock(t *testing.T) {
	mconn := &mockconn.MockConn{}
	conn := NewConn(mconn, time.Second, time.Second)
	assert.Nil(t, conn.SetDeadline(time.Now()))
	assert.Nil(t, conn.SetReadDeadline(time.Now()))
	assert.Nil(t, conn.SetWriteDeadline(time.Now()))
	assert.Nil(t, conn.Close())
	assert.Equal(t, conn.LocalAddr(), conn.RemoteAddr())
}

func _testReadWriteWithMockTimeout(t *testing.T, rt, wt time.Duration) {
	data := []byte("Bilibili 干杯 - ( ゜- ゜)つロ")
	mconn := mockconn.CreateConn(data, 1)
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
	mconn := mockconn.CreateConn(data, 1)
	conn := NewConn(mconn, time.Second, time.Second)

	recv := make([]byte, len(data))
	_, err := conn.Read(recv)
	assert.NoError(t, err)
	_, err = conn.Write(data)
	assert.NoError(t, err)

	// TODO(wayslog): reexport change attributes as a function
	conn.readTimeout = 0
	conn.writeTimeout = 0
	mconn = mockconn.CreateConn(data, 1)
	conn.Conn = mconn

	recv = make([]byte, len(data))
	_, err = conn.Read(recv)
	assert.NoError(t, err)
	_, err = conn.Write(data)
	assert.NoError(t, err)
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

func TestConnNoConn(t *testing.T) {
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
	conn.Conn = nil

	bs := make([]byte, 1)
	n, err := conn.Read(bs)
	assert.Equal(t, 0, n)
	assert.Equal(t, ErrConnClosed, err)

	n, err = conn.Write(bs)
	assert.Equal(t, 0, n)
	assert.Equal(t, ErrConnClosed, err)

	buffers := net.Buffers([][]byte{[]byte("baka"), []byte("qiu")})
	n64, err := conn.Writev(&buffers)
	assert.Equal(t, int64(0), n64)
	assert.Equal(t, ErrConnClosed, err)
}
