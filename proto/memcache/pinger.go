package memcache

import (
	"bytes"
	"sync/atomic"

	"overlord/lib/bufio"
	libnet "overlord/lib/net"
	"overlord/proto"

	"github.com/pkg/errors"
)

const (
	pingBufferSize = 8
)

var (
	pingBytes = []byte("set _ping 0 0 4\r\npong\r\n")
	pongBytes = []byte("STORED\r\n")
)

type mcPinger struct {
	conn *libnet.Conn
	bw   *bufio.Writer
	br   *bufio.Reader

	state int32
}

// NewPinger new pinger.
func NewPinger(nc *libnet.Conn) proto.Pinger {
	return &mcPinger{
		conn: nc,
		bw:   bufio.NewWriter(nc),
		br:   bufio.NewReader(nc, bufio.NewBuffer(pingBufferSize)),
	}
}

func (m *mcPinger) Ping() (err error) {
	if atomic.LoadInt32(&m.state) == closed {
		err = errors.WithStack(ErrPingerPong)
		return
	}
	if err = m.bw.Write(pingBytes); err != nil {
		err = errors.WithStack(err)
		return
	}
	if err = m.bw.Flush(); err != nil {
		err = errors.WithStack(err)
		return
	}
	_ = m.br.Read()
	var b []byte
	if b, err = m.br.ReadLine(); err != nil {
		err = errors.WithStack(err)
		return
	}
	if !bytes.Equal(b, pongBytes) {
		err = errors.WithStack(ErrPingerPong)
	}
	return
}

func (m *mcPinger) Close() error {
	if atomic.CompareAndSwapInt32(&m.state, opened, closed) {
		return m.conn.Close()
	}
	return nil
}
