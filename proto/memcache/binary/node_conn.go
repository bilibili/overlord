package binary

import (
	"bytes"
	"encoding/binary"
	"sync/atomic"
	"time"

	"overlord/lib/bufio"
	libnet "overlord/lib/net"
	"overlord/lib/prom"
	"overlord/proto"

	"github.com/pkg/errors"
)

const (
	opened = int32(0)
	closed = int32(1)
)

type nodeConn struct {
	cluster string
	addr    string

	conn *libnet.Conn
	bw   *bufio.Writer
	br   *bufio.Reader

	state int32
}

// NewNodeConn returns node conn.
func NewNodeConn(cluster, addr string, dialTimeout, readTimeout, writeTimeout time.Duration) (nc proto.NodeConn) {
	conn := libnet.DialWithTimeout(addr, dialTimeout, readTimeout, writeTimeout)
	nc = &nodeConn{
		cluster: cluster,
		addr:    addr,
		conn:    conn,
		bw:      bufio.NewWriter(conn),
		br:      bufio.NewReader(conn, nil),
	}
	return
}

func (n *nodeConn) WriteBatch(mb *proto.MsgBatch) (err error) {
	if n.Closed() {
		err = errors.Wrap(ErrClosed, "MC Writer write")
		return
	}
	var (
		m   *proto.Message
		idx int
	)
	for {
		m = mb.Nth(idx)
		if m == nil {
			break
		}
		err = n.write(m)
		if err != nil {
			m.WithError(err)
			return err
		}
		m.MarkWrite()
		idx++
	}
	return
}


func (n *nodeConn) Flush() (err error) {
	if err = n.bw.Flush(); err != nil {
		err = errors.Wrap(err, "MC binary Writer flush message bytes")
	}
	return
}


func (n *nodeConn) write(m *proto.Message) (err error) {
	if n.Closed() {
		err = errors.Wrap(ErrClosed, "MC Writer write")
		return
	}
	mcr, ok := m.Request().(*MCRequest)
	if !ok {
		err = errors.Wrap(ErrAssertReq, "MC Writer assert request")
		return
	}
	_ = n.bw.Write(magicReqBytes)

	cmd := mcr.rTp
	if cmd == RequestTypeGetQ || cmd == RequestTypeGetKQ {
		cmd = RequestTypeGetK
	}
	_ = n.bw.Write(cmd.Bytes())
	_ = n.bw.Write(mcr.keyLen)
	_ = n.bw.Write(mcr.extraLen)
	_ = n.bw.Write(zeroBytes)
	_ = n.bw.Write(zeroTwoBytes)
	_ = n.bw.Write(mcr.bodyLen)
	_ = n.bw.Write(mcr.opaque)
	_ = n.bw.Write(mcr.cas)
	if !bytes.Equal(mcr.bodyLen, zeroFourBytes) {
		_ = n.bw.Write(mcr.data)
	}
	return
}

func (n *nodeConn) ReadBatch(mb *proto.MsgBatch) (err error) {
	if n.Closed() {
		err = errors.Wrap(ErrClosed, "MC Reader read batch message")
		return
	}
	defer n.br.ResetBuffer(nil)
	n.br.ResetBuffer(mb.Buffer())
	var (
		size   int
		cursor int
		nth    int
		m      *proto.Message

		mcr *MCRequest
		ok  bool
	)
	m = mb.Nth(nth)
	mcr, ok = m.Request().(*MCRequest)
	if !ok {
		err = errors.Wrap(ErrAssertReq, "MC Reader assert request")
		return
	}
	for {
		err = n.br.Read()
		if err != nil {
			err = errors.Wrap(err, "MC Reader while read")
			return
		}
		for {
			size, err = n.fillMCRequest(mcr, n.br.Buffer().Bytes()[cursor:])
			if err == bufio.ErrBufferFull {
				err = nil
				break
			} else if err != nil {
				return
			}
			m.MarkRead()

			cursor += size
			nth++

			m = mb.Nth(nth)
			if m == nil {
				return
			}
			mcr, ok = m.Request().(*MCRequest)
			if !ok {
				err = errors.Wrap(ErrAssertReq, "MC Reader assert request")
				return
			}
		}
	}
}

func (n *nodeConn) fillMCRequest(mcr *MCRequest, data []byte) (size int, err error) {
	if len(data) < requestHeaderLen {
		return 0, bufio.ErrBufferFull
	}
	parseHeader(data[0:requestHeaderLen], mcr, false)

	bl := binary.BigEndian.Uint32(mcr.bodyLen)
	if bl == 0 {
		size = requestHeaderLen
		return
	}
	if len(data[requestHeaderLen:]) < int(bl) {
		return 0, bufio.ErrBufferFull
	}
	size = requestHeaderLen + int(bl)
	mcr.data = data[requestHeaderLen : requestHeaderLen+bl]

	if mcr.rTp == RequestTypeGet || mcr.rTp == RequestTypeGetQ || mcr.rTp == RequestTypeGetK || mcr.rTp == RequestTypeGetKQ {
		if prom.On {
			prom.Hit(n.cluster, n.addr)
		}
	}
	return
}

func (n *nodeConn) Close() error {
	if atomic.CompareAndSwapInt32(&n.state, opened, closed) {
		return n.conn.Close()
	}
	return nil
}

func (n *nodeConn) Closed() bool {
	return atomic.LoadInt32(&n.state) == closed
}
