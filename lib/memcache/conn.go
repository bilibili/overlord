package memcache

import (
	"bytes"
	"fmt"
	"time"

	"overlord/lib/bufio"
	"overlord/lib/net"

	"github.com/pkg/errors"
)

// Conn mc conn
type Conn struct {
	conn *net.Conn
	bw   *bufio.Writer
	br   *bufio.Reader
}

var (
	pingBytes = []byte("set _ping 0 0 4\r\npong\r\n")
	pongBytes = []byte("STORED\r\n")
	errping   = fmt.Errorf("get pong err")
)

// New mc conn.
func New(addr string, dialTimeout, writeTimeout, readTimeout time.Duration) *Conn {
	c := &Conn{
		conn: net.DialWithTimeout(addr, dialTimeout, readTimeout, writeTimeout),
	}
	c.br = bufio.NewReader(c.conn, bufio.NewBuffer(1024))
	c.bw = bufio.NewWriter(c.conn)
	return c
}

// Ping mc server.
func (c *Conn) Ping() (err error) {
	if err = c.bw.Write(pingBytes); err != nil {
		err = errors.WithStack(err)
		return
	}
	if err = c.bw.Flush(); err != nil {
		err = errors.WithStack(err)
		return
	}
	_ = c.br.Read()
	var b []byte
	if b, err = c.br.ReadLine(); err != nil {
		err = errors.WithStack(err)
		return
	}
	if !bytes.Equal(b, pongBytes) {
		err = errors.WithStack(errping)
	}
	return
}

// Close conn.
func (c *Conn) Close() error {
	return c.conn.Close()
}
