package memcache

import (
	"bufio"
	"bytes"
	"net"
	"sync/atomic"
	"time"

	"github.com/felixhao/overlord/proto"
	"github.com/pkg/errors"
)

const (
	pingerOpening = int32(0)
	pingerClosed  = int32(1)
)

type pinger struct {
	conn net.Conn
	br   *bufio.Reader

	addr         string
	dialTimeout  time.Duration
	readTimeout  time.Duration
	writeTimeout time.Duration

	closed int32
}

// NewPinger returns pinger.
func NewPinger(addr string, dialTimeout, readTimeout, writeTimeout time.Duration) (p proto.Pinger) {
	per := &pinger{
		addr:         addr,
		dialTimeout:  dialTimeout,
		readTimeout:  readTimeout,
		writeTimeout: writeTimeout,
	}
	per.reconn()
	p = per
	return
}

func (p *pinger) reconn() error {
	conn, err := net.DialTimeout("tcp", p.addr, p.dialTimeout)
	if err != nil {
		return err
	}
	p.conn = conn
	if p.br != nil {
		p.br.Reset(conn)
	} else {
		p.br = bufio.NewReader(conn)
	}
	return nil
}

func (p *pinger) Ping() (err error) {
	if p.Closed() {
		err = errors.Wrap(ErrClosed, "MC Pinger Ping")
		return
	}
	defer func() {
		if err != nil {
			p.reconn()
		}
	}()
	const (
		ping = "set ping 0 0 4\r\npong\r\n"
		pong = "STORED\r\n"
	)
	if p.conn == nil {
		if err = p.reconn(); err != nil {
			return
		}
	}
	if p.writeTimeout > 0 {
		p.conn.SetWriteDeadline(time.Now().Add(p.writeTimeout))
	}
	if _, err = p.conn.Write([]byte(ping)); err != nil {
		err = errors.Wrap(err, "MC Pinger ping flush bytes")
		return
	}
	if p.readTimeout > 0 {
		p.conn.SetReadDeadline(time.Now().Add(p.readTimeout))
	}
	bs, err := p.br.ReadSlice(delim)
	if err != nil {
		err = errors.Wrap(err, "MC Pinger ping read response bytes")
		return
	}
	if !bytes.Equal([]byte(pong), bs) {
		err = errors.Wrap(ErrPingerPong, "MC Pinger ping check response bytes")
	}
	return
}

func (p *pinger) Close() error {
	if atomic.CompareAndSwapInt32(&p.closed, handlerOpening, handlerClosed) {
		if p.conn != nil {
			return p.conn.Close()
		}
	}
	return nil
}

func (p *pinger) Closed() bool {
	return atomic.LoadInt32(&p.closed) == handlerClosed
}
