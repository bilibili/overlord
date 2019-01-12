package binary

import (
	"bytes"
	"sync/atomic"

	"overlord/pkg/bufio"
	libnet "overlord/pkg/net"
	"overlord/proxy/proto"

	"github.com/pkg/errors"
)

const (
	pingBufferSize = 24
)

var (
	pingBs = []byte{
		0x80,       // magic
		0x0a,       // cmd: noop
		0x00, 0x00, // key len
		0x00,       // extra len
		0x00,       // data type
		0x00, 0x00, // vbucket
		0x00, 0x00, 0x00, 0x00, // body len
		0x00, 0x00, 0x00, 0x00, // opaque
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // cas
	}
	pongBs = []byte{
		0x81,       // magic
		0x0a,       // cmd: noop
		0x00, 0x00, // key len
		0x00,       // extra len
		0x00,       // data type
		0x00, 0x00, // status
		0x00, 0x00, 0x00, 0x00, // body len
		0x00, 0x00, 0x00, 0x00, // opaque
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // cas
	}
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
	_ = m.bw.Write(pingBs)
	if err = m.bw.Flush(); err != nil {
		err = errors.WithStack(err)
		return
	}
	_ = m.br.Read()
	defer m.br.AdvanceTo(0)
	head, err := m.br.ReadExact(requestHeaderLen)
	if err != nil {
		err = errors.WithStack(err)
		return
	}
	if !bytes.Equal(head, pongBs) {
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
