package redis

import (
	"bytes"
	"errors"
	"sync/atomic"

	"overlord/lib/bufio"
	libnet "overlord/lib/net"
)

// errors
var (
	ErrPingClosed = errors.New("ping interface has been closed")
	ErrBadPong    = errors.New("pong response payload is bad")
)

var (
	pingBytes = []byte("*1\r\n$4\r\nPING\r\n")
	pongBytes = []byte("+PONG\r\n")
)

type pinger struct {
	conn *libnet.Conn

	br *bufio.Reader
	bw *bufio.Writer

	state uint32
}

func newPinger(conn *libnet.Conn) *pinger {
	return &pinger{
		conn:  conn,
		br:    bufio.NewReader(conn, bufio.Get(64)),
		bw:    bufio.NewWriter(conn),
		state: opened,
	}
}

func (p *pinger) ping() (err error) {
	if atomic.LoadUint32(&p.state) == closed {
		err = ErrPingClosed
		return
	}
	_ = p.bw.Write(pingBytes)
	if err = p.bw.Flush(); err != nil {
		return err
	}
	_ = p.br.Read()
	data, err := p.br.ReadLine()
	if err != nil {
		return
	}
	if !bytes.Equal(data, pongBytes) {
		err = ErrBadPong
	}
	return
}

func (p *pinger) Close() error {
	if atomic.CompareAndSwapUint32(&p.state, opened, closed) {
		return p.conn.Close()
	}
	return nil
}
