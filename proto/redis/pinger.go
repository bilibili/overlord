package redis

import (
	"errors"
	"sync/atomic"
	"time"

	"bytes"

	"github.com/felixhao/overlord/proto"
)

const (
	pingerOpening = int32(0)
	pingerClosed  = int32(1)
)

// errors
var (
	ErrPingPong = errors.New("ping pong fail to redis")
)

var pongBytes = []byte("PONG")

// NewPinger will create new pinger of redis
func NewPinger(addr string, dialTimeout, readTimeout, writeTimeout time.Duration) (p proto.Pinger) {
	connFunc := func(p *pinger) error {
		conn, err := dialWithTimeout(p.addr, dialTimeout, readTimeout, writeTimeout)
		if err != nil {
			return err
		}
		p.conn = conn
		p.buf = newBuffer(conn, conn)
		return nil
	}

	pg := &pinger{
		addr:     addr,
		closed:   pingerOpening,
		connFunc: connFunc,
	}

	p = pg
	return
}

type pinger struct {
	addr string
	conn *connection
	buf  *buffer

	closed int32

	connFunc func(*pinger) error
}

func (p *pinger) Ping() error {
	if p.Closed() {
		return ErrReuseClosedConn
	}

	if p.conn == nil || p.buf == nil {
		err := p.connFunc(p)
		if err != nil {
			return err
		}
	}

	cmd := NewCommand("PING")

	err := p.buf.encodeResp(cmd.respObj)
	if err != nil {
		return err
	}

	err = p.buf.Flush()
	if err != nil {
		return err
	}

	reply, err := p.buf.decodeRespObj()
	if err != nil {
		return err
	}

	if !p.checkPong(reply) {
		return ErrPingPong
	}

	return nil
}

func (p *pinger) checkPong(robj *resp) bool {
	return robj.rtype == respString && bytes.Equal(robj.data, pongBytes)
}

func (p *pinger) Close() error {
	if atomic.CompareAndSwapInt32(&p.closed, pingerOpening, pingerClosed) {
		if p.conn != nil {
			_ = p.buf.Flush()
			p.buf = nil
			_ = p.conn.Close()
			p.conn = nil
		}
	}
	return nil
}

func (p *pinger) Closed() bool {
	return atomic.LoadInt32(&p.closed) == pingerClosed
}
