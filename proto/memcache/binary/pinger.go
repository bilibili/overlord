package binary

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
	_ = m.bw.Write(magicReqBytes)
	_ = m.bw.Write(noopBytes)
	_ = m.bw.Write(zeroTwoBytes)
	_ = m.bw.Write(zeroBytes)
	_ = m.bw.Write(zeroBytes)
	_ = m.bw.Write(zeroTwoBytes)
	_ = m.bw.Write(zeroFourBytes)
	_ = m.bw.Write(zeroFourBytes)
	_ = m.bw.Write(zeroEightBytes)
	if err = m.bw.Flush(); err != nil {
		err = errors.Wrap(err, "MC ping flush")
		return
	}
	err = m.br.Read()
	head, err := m.br.ReadExact(requestHeaderLen)
	if err != nil {
		err = errors.Wrap(err, "MC ping read exact")
		return
	}
	if !bytes.Equal(head[6:8], zeroTwoBytes) {
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
