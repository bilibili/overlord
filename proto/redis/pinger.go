package redis

import (
	"bytes"
	"errors"
	"overlord/lib/bufio"
	libnet "overlord/lib/net"
	"sync/atomic"
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
		conn: conn,
		br:   bufio.NewReader(conn, bufio.Get(64)),
		bw:   bufio.NewWriter(conn),
	}
}

func (p *pinger) ping() (err error) {
	if atomic.LoadUint32(&p.state) == closed {
		err = ErrPingClosed
		return
	}
	err = p.bw.Write(pingBytes)
	if err != nil {
		return
	}
	data, err := p.br.ReadUntil(lfByte)
	if err != nil {
		return
	}
	if bytes.Equal(data, pongBytes) {
		err = ErrBadPong
		return
	}
	return
}