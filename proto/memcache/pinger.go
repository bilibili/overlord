package memcache

import (
	"bytes"
	"sync/atomic"

	"overlord/lib/bufio"
	libnet "overlord/lib/net"

	"github.com/pkg/errors"
)

const (
	pingBufferSize = 32
)

var (
	ping = []byte("set _ping 0 0 4\r\npong\r\n")
	pong = []byte("STORED\r\n")
)

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
	if err = m.bw.Write(ping); err != nil {
		err = errors.Wrap(err, "MC ping write")
		return
	}
	if err = m.bw.Flush(); err != nil {
		err = errors.Wrap(err, "MC ping flush")
		return
	}
	_ = m.br.Read()
	var b []byte
	if b, err = m.br.ReadLine(); err != nil {
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
