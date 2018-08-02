package net

import (
	"errors"
	"net"
	"overlord/lib/log"
	"time"
)

var (
	// ErrConnClosed error connection closed.
	ErrConnClosed = errors.New("connection is closed")
)

// Conn is a net.Conn self implement
// Add auto timeout setting.
type Conn struct {
	addr string
	net.Conn

	dialTimeout  time.Duration
	readTimeout  time.Duration
	writeTimeout time.Duration

	closed bool
	err    error
}

// DialWithTimeout will create new auto timeout Conn
func DialWithTimeout(addr string, dialTimeout, readTimeout, writeTimeout time.Duration) (c *Conn) {
	sock, err := net.DialTimeout("tcp", addr, dialTimeout)
	if err != nil {
		log.Errorf("dialtimeout  %s fail", addr)
		time.Sleep(time.Second)
		sock, err = net.DialTimeout("tcp", addr, dialTimeout)
	}
	c = &Conn{addr: addr, Conn: sock, dialTimeout: dialTimeout, readTimeout: readTimeout, writeTimeout: writeTimeout, err: err}
	return
}

// NewConn will create new Connection with given socket
func NewConn(sock net.Conn, readTimeout, writeTimeout time.Duration) (c *Conn) {
	c = &Conn{Conn: sock, readTimeout: readTimeout, writeTimeout: writeTimeout}
	return
}

// Dup will re-dial to the given addr by using timeouts stored in itself.
func (c *Conn) Dup() *Conn {
	return DialWithTimeout(c.addr, c.dialTimeout, c.readTimeout, c.writeTimeout)
}

// ReConnect re connect.
func (c *Conn) ReConnect() (err error) {
	if c.Conn != nil {
		c.Conn.Close()
	}
	if c.addr == "" || c.closed {
		return
	}
	conn := DialWithTimeout(c.addr, c.dialTimeout, c.readTimeout, c.writeTimeout)
	c = conn
	err = c.err
	return
}

func (c *Conn) Read(b []byte) (n int, err error) {
	if c.closed || c.Conn == nil {
		return 0, ErrConnClosed
	}
	if c.err != nil && c.addr != "" {
		if re := c.ReConnect(); re != nil {
			err = c.err
			return
		}
		c.err = nil
	}
	if timeout := c.readTimeout; timeout != 0 {
		if err = c.SetReadDeadline(time.Now().Add(timeout)); err != nil {
			c.err = err
			return
		}
	}

	n, err = c.Conn.Read(b)
	c.err = err
	return
}

func (c *Conn) Write(b []byte) (n int, err error) {
	if c.closed || c.Conn == nil {
		return 0, ErrConnClosed
	}
	if c.err != nil && c.addr != "" {
		if re := c.ReConnect(); re != nil {
			err = c.err
			return
		}
		c.err = nil
	}
	if timeout := c.writeTimeout; timeout != 0 {
		if err = c.SetWriteDeadline(time.Now().Add(timeout)); err != nil {
			c.err = err
			return
		}
	}
	n, err = c.Conn.Write(b)
	c.err = err
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
	if c.err != nil && c.addr != "" {
		if re := c.ReConnect(); re != nil {
			return 0, c.err
		}
		c.err = nil
	}
	n, err := buf.WriteTo(c.Conn)
	c.err = err
	return n, err
}
