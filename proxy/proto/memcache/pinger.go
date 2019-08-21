package memcache

import (
	"bytes"
	"sync/atomic"

	"overlord/pkg/bufio"
	libnet "overlord/pkg/net"
	"overlord/proxy/proto"

	"github.com/pkg/errors"
)

const (
	pingBufferSize = 128
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
		br:   bufio.NewReader(nc, bufio.NewBuffer(pingBufferSize)),
		bw:   bufio.NewWriter(nc),
	}
}

func (m *mcPinger) Ping() (err error) {
	if atomic.LoadInt32(&m.state) == closed {
		err = errors.WithStack(ErrPingerPong)
		return
	}
	m.bw.Write(pingBytes)
	if err = m.bw.Flush(); err != nil {
		err = errors.WithStack(err)
		return
	}
	_ = m.br.Read()
	defer m.br.Buffer().Reset()
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
