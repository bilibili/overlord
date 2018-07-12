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
	_ = m.bw.Write(magicReqBytes)  // NOTE: magic
	_ = m.bw.Write(noopBytes)      // NOTE: cmd
	_ = m.bw.Write(zeroTwoBytes)   // NOTE: key len
	_ = m.bw.Write(zeroBytes)      // NOTE: extra len
	_ = m.bw.Write(zeroBytes)      // NOTE: data type
	_ = m.bw.Write(zeroBytes)      // NOTE: vbucket
	_ = m.bw.Write(zeroFourBytes)  // NOTE: total body
	_ = m.bw.Write(zeroFourBytes)  // NOTE: opaque
	_ = m.bw.Write(zeroEightBytes) // NOTE: cas
	if err = m.bw.Flush(); err != nil {
		err = errors.Wrap(err, "MC ping flush")
		return
	}
	if err = m.br.Read(); err != nil {
		err = errors.Wrap(err, "MC ping read")
		return
	}
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
