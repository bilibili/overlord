package binary

import (
	"bytes"
	"sync/atomic"

	"overlord/lib/bufio"
	libnet "overlord/lib/net"

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
	conn   *libnet.Conn
	bw     *bufio.Writer
	br     *bufio.Reader
	closed int32
}

func newMCPinger(nc *libnet.Conn) *mcPinger {
	return &mcPinger{
		conn: nc,
		bw:   bufio.NewWriter(nc),
		br:   bufio.NewReader(nc, bufio.NewBuffer(pingBufferSize)),
	}
}

func (m *mcPinger) Ping() (err error) {
	if atomic.LoadInt32(&m.closed) == handlerClosed {
		err = ErrPingerPong
		return
	}
	_ = m.bw.Write(pingBs)
	if err = m.bw.Flush(); err != nil {
		err = errors.Wrap(err, "MC ping flush")
		return
	}
	_ = m.br.Read()
	head, err := m.br.ReadExact(requestHeaderLen)
	if err != nil {
		err = errors.Wrap(err, "MC ping read exact")
		return
	}
	if !bytes.Equal(head, pongBs) {
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
