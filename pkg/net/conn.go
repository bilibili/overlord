package net

import (
	"errors"
	"net"
	"sync/atomic"
	"time"
)

var (
	// ErrConnClosed error connection closed.
	ErrConnClosed       = errors.New("connection is closed")
	ConnectionCnt int64 = 0
)

// Conn is a net.Conn self implement
// Add auto timeout setting.
type Conn struct {
	addr string
	net.Conn

	dialTimeout  time.Duration
	readTimeout  time.Duration
	writeTimeout time.Duration
	ID           int64

	closed bool
}

// DialWithTimeout will create new auto timeout Conn
func DialWithTimeout(addr string, dialTimeout, readTimeout, writeTimeout time.Duration) (c *Conn) {
	sock, _ := net.DialTimeout("tcp", addr, dialTimeout)
	c = &Conn{addr: addr, Conn: sock, dialTimeout: dialTimeout, readTimeout: readTimeout, writeTimeout: writeTimeout}
	return
}

// NewConn will create new Connection with given socket
func NewConn(sock net.Conn, readTimeout, writeTimeout time.Duration) (c *Conn) {
	var id = atomic.AddInt64(&ConnectionCnt, 1)
	c = &Conn{Conn: sock, readTimeout: readTimeout, writeTimeout: writeTimeout, ID: id}
	return
}

// Dup will re-dial to the given addr by using timeouts stored in itself.
func (c *Conn) Dup() *Conn {
	return DialWithTimeout(c.addr, c.dialTimeout, c.readTimeout, c.writeTimeout)
}

func (c *Conn) Read(b []byte) (n int, err error) {
	if c.closed || c.Conn == nil {
		return 0, ErrConnClosed
	}
	if timeout := c.readTimeout; timeout != 0 {
		if err = c.SetReadDeadline(time.Now().Add(timeout)); err != nil {
			return
		}
	}
	n, err = c.Conn.Read(b)
	return
}

func (c *Conn) Write(b []byte) (n int, err error) {
	if c.closed || c.Conn == nil {
		return 0, ErrConnClosed
	}
	if timeout := c.writeTimeout; timeout != 0 {
		if err = c.SetWriteDeadline(time.Now().Add(timeout)); err != nil {
			return
		}
	}
	n, err = c.Conn.Write(b)
	return
}

// Close close conn.
func (c *Conn) Close() error {
	if c.Conn != nil && !c.closed {
		c.closed = true
		return c.Conn.Close()
	}
	return nil
}

// Writev impl the net.buffersWriter to support writev
func (c *Conn) Writev(buf *net.Buffers) (int64, error) {
	if c.closed || c.Conn == nil {
		return 0, ErrConnClosed
	}
	n, err := buf.WriteTo(c.Conn)
	return n, err
}
