package memcache

import (
	"bytes"
	"sync/atomic"

	"github.com/felixhao/overlord/lib/bufio"
	libnet "github.com/felixhao/overlord/lib/net"
	"github.com/pkg/errors"
)

const (
	pingBufferSize = 32
	ping           = "set _ping 0 0 4\r\npong\r\n"
)

var pong = []byte("STORED\r\n")

type mcPinger struct {
	conn   *libnet.Conn
	bw     *bufio.Writer
	br     *bufio.Reader
	closed int32
}

func newMCPinger(nc *libnet.Conn) *mcPinger {
	return &mcPinger{
		conn: nc,
		bw:   bufio.NewWriter(nc),
		br:   bufio.NewReader(nc, bufio.Get(pingBufferSize)),
	}
}

func (m *mcPinger) Ping() (err error) {
	if atomic.LoadInt32(&m.closed) == handlerClosed {
		err = ErrPingerPong
		return
	}
	if err = m.bw.WriteString(ping); err != nil {
		err = errors.Wrap(err, "MC ping write")
		return
	}
	if err = m.bw.Flush(); err != nil {
		err = errors.Wrap(err, "MC ping flush")
		return
	}
	var b []byte
	if b, err = m.br.ReadUntil(delim); err != nil {
		err = errors.Wrap(err, "MC ping read response")
		return
	}
	if !bytes.Equal(b, pong) {
		err = ErrPingerPong
	}
	return
}

func (m *mcPinger) Close() error {
	if atomic.CompareAndSwapInt32(&m.closed, handlerOpening, handlerClosed) {
		return m.conn.Close()
	}
	return nil
}
