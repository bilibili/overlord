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

	hasDeadline bool

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

func (c *connection) CloseReader() error {
	if t, ok := c.sock.(*net.TCPConn); ok {
		return t.CloseRead()
	}
	return c.Close()
}

func (c *connection) SetKeepAlivePeriod(d time.Duration) error {
	if t, ok := c.sock.(*net.TCPConn); ok {
		if err := t.SetKeepAlive(d != 0); err != nil {
			return err
		}
		if d != 0 {
			if err := t.SetKeepAlivePeriod(d); err != nil {
				return err
			}
		}
	}
	return nil
}

func (c *connection) Read(b []byte) (int, error) {
	if timeout := c.readerTimeout; timeout != 0 {
		if err := c.sock.SetReadDeadline(time.Now().Add(timeout)); err != nil {
			return 0, err
		}
		c.hasDeadline = true
	} else if c.hasDeadline {
		if err := c.sock.SetReadDeadline(time.Time{}); err != nil {
			return 0, err
		}
		c.hasDeadline = false
	}
	return c.sock.Read(b)
}

func (c *connection) Write(b []byte) (int, error) {
	if timeout := c.writerTimeout; timeout != 0 {
		if err := c.sock.SetWriteDeadline(time.Now().Add(timeout)); err != nil {
			return 0, err
		}
		c.hasDeadline = true
	} else if c.hasDeadline {
		if err := c.sock.SetWriteDeadline(time.Time{}); err != nil {
			return 0, err
		}
		c.hasDeadline = false
	}
	n, err := c.sock.Write(b)
	if err != nil {
		return n, err
	}
	c.LastWrite = time.Now()
	return n, err
}
