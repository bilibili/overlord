package redis

import (
	"net"
	"time"
)

// connection is a net.connection self implement
type connection struct {
	sock net.Conn

	readerTimeout time.Duration
	writerTimeout time.Duration

	hasReadDeadline  bool
	hasWriteDeadline bool

	LastWrite time.Time
}

func dialWithTimeout(addr string, dialTimeout, readerTimeout, writerTimeout time.Duration) (*connection, error) {
	socket, err := net.DialTimeout("tcp", addr, dialTimeout)
	if err != nil {
		return nil, err
	}
	return newConn(socket, readerTimeout, writerTimeout), nil
}

func newConn(sock net.Conn, readerTimeout, writerTimeout time.Duration) *connection {
	connection := &connection{sock: sock, readerTimeout: readerTimeout, writerTimeout: writerTimeout}
	return connection
}

func (c *connection) LocalAddr() string {
	return c.sock.LocalAddr().String()
}

func (c *connection) RemoteAddr() string {
	return c.sock.RemoteAddr().String()
}

func (c *connection) Close() error {
	return c.sock.Close()
}

// SetDeadline sets the read and write deadlines associated
// sockets.
func (c *connection) SetDeadline(t time.Time) error {
	return c.sock.SetDeadline(t)
}

// SetReadDeadline sets the deadline for future Read calls
// and any currently-blocked Read call.
// A zero value for t means Read will not time out.
func (c *connection) SetReadDeadline(t time.Time) error {
	return c.sock.SetReadDeadline(t)
}

// SetWriteDeadline sets the deadline for future Write calls
// and any currently-blocked Write call.
// Even if write times out, it may return n > 0, indicating that
// some of the data was successfully written.
// A zero value for t means Write will not time out.
func (c *connection) SetWriteDeadline(t time.Time) error {
	return c.sock.SetWriteDeadline(t)
}

func (c *connection) CloseReader() error {
	if t, ok := c.sock.(*net.TCPConn); ok {
		return t.CloseRead()
	}
	return c.Close()
}

func (c *connection) Read(b []byte) (int, error) {
	if timeout := c.readerTimeout; timeout != 0 {
		if err := c.SetReadDeadline(time.Now().Add(timeout)); err != nil {
			return 0, err
		}
		c.hasReadDeadline = true
	} else if c.hasReadDeadline {
		if err := c.SetReadDeadline(time.Time{}); err != nil {
			return 0, err
		}
		c.hasReadDeadline = false
	}
	return c.sock.Read(b)
}

func (c *connection) Write(b []byte) (int, error) {
	if timeout := c.writerTimeout; timeout != 0 {
		if err := c.SetWriteDeadline(time.Now().Add(timeout)); err != nil {
			return 0, err
		}
		c.hasWriteDeadline = true
	} else if c.hasWriteDeadline {
		if err := c.SetWriteDeadline(time.Time{}); err != nil {
			return 0, err
		}
		c.hasWriteDeadline = false
	}
	n, err := c.sock.Write(b)
	if err != nil {
		return n, err
	}
	c.LastWrite = time.Now()
	return n, err
}
