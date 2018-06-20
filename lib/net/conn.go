package net

import (
	"net"
	"time"
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
	c = &Conn{Conn: sock, dialTimeout: dialTimeout, readTimeout: readTimeout, writeTimeout: writeTimeout, err: err}
	return
}

// NewConn will create new Connection with given socket
func NewConn(sock net.Conn, readTimeout, writeTimeout time.Duration) (c *Conn) {
	c = &Conn{Conn: sock, readTimeout: readTimeout, writeTimeout: writeTimeout}
	return
}

// Dup will re-dial to the given addr by using timeouts stored in itself.
func (c *Conn) Dup() *Conn {
	return DialWithTimeout(c.RemoteAddr().String(), c.dialTimeout, c.readTimeout, c.writeTimeout)
}

// ReConnect re connect.
func (c *Conn) ReConnect() (err error) {
	if c.Conn != nil {
		c.Conn.Close()
	}
	if c.addr == "" {
		return
	}
	if c.closed {
		return
	}
	sock, err := net.DialTimeout("tcp", c.addr, c.dialTimeout)
	if err != nil {
		return err
	}
	c.Conn = sock
	return
}

func (c *Conn) Read(b []byte) (n int, err error) {
	if c.closed {
		return
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

// writeBuffers impl the net.buffersWriter to support writev
func (c *Conn) writeBuffers(buf *net.Buffers) (int64, error) {
	return buf.WriteTo(c.Conn)
}
