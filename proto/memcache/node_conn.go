package memcache

import (
	"bytes"
	"io"
	"sync/atomic"
	"time"

	"overlord/lib/bufio"
	libnet "overlord/lib/net"
	"overlord/lib/prom"
	"overlord/proto"

	"github.com/pkg/errors"
)

const (
	handlerOpening = int32(0)
	handlerClosed  = int32(1)
)

type nodeConn struct {
	cluster string
	addr    string
	conn    *libnet.Conn
	bw      *bufio.Writer
	br      *bufio.Reader
	closed  int32

	pinger *mcPinger
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
		pinger:  newMCPinger(conn),
	}
	return
}

// Ping will send some special command by checking mc node is alive
func (n *nodeConn) Ping() (err error) {
	if n.Closed() {
		err = io.EOF
		return
	}
	err = n.pinger.Ping()
	return
}

func (n *nodeConn) WriteBatch(mb *proto.MsgBatch) (err error) {
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
			m.DoneWithError(err)
			return err
		}
		m.MarkWrite()
		idx++
	}

	if err = n.bw.Flush(); err != nil {
		err = errors.Wrap(err, "MC Writer handle flush Msg bytes")
	}
	return
}

func (n *nodeConn) write(m *proto.Message) (err error) {
	if n.Closed() {
		err = errors.Wrap(ErrClosed, "MC Writer conn closed")
		return
	}
	mcr, ok := m.Request().(*MCRequest)
	if !ok {
		err = errors.Wrap(ErrAssertReq, "MC Writer assert request")
		return
	}
	_ = n.bw.Write(mcr.rTp.Bytes())
	_ = n.bw.Write(spaceBytes)
	if mcr.rTp == RequestTypeGat || mcr.rTp == RequestTypeGats {
		_ = n.bw.Write(mcr.data) // NOTE: exp time
		_ = n.bw.Write(spaceBytes)
		_ = n.bw.Write(mcr.key)
		_ = n.bw.Write(crlfBytes)
	} else {
		_ = n.bw.Write(mcr.key)
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
		err = errors.Wrap(ErrAssertReq, "MC Writer assert request")
		return
	}
	for {
		err = n.br.Read()
		if err != nil {
			err = errors.Wrap(err, "MC Reader node conn while read")
			return
		}
		for {
			size, err = n.fillMCRequest(mcr, n.br.Buffer().Bytes()[cursor:])
			if err == bufio.ErrBufferFull {
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
				err = errors.Wrap(ErrAssertReq, "MC Writer assert request")
				return
			}
		}
	}
}

func (n *nodeConn) fillMCRequest(mcr *MCRequest, data []byte) (size int, err error) {
	pos := bytes.IndexByte(data, delim)
	if pos == -1 {
		return 0, bufio.ErrBufferFull
	}

	bs := data[:pos+1]
	size = len(bs)
	mcr.data = bs
	if _, ok := withValueTypes[mcr.rTp]; !ok {
		return
	}

	if bytes.Equal(bs, endBytes) {
		prom.Miss(n.cluster, n.addr)
		return
	}
	if prom.On() {
		prom.Hit(n.cluster, n.addr)
	}
	length, err := findLength(bs, mcr.rTp == RequestTypeGets || mcr.rTp == RequestTypeGats)
	if err != nil {
		err = errors.Wrap(err, "MC Handler while parse length")
		return
	}

	size += length + 2 + len(endBytes)
	if len(data) < size {
		return 0, bufio.ErrBufferFull
	}
	mcr.data = data[:size]
	return
}

func (n *nodeConn) Close() error {
	if atomic.CompareAndSwapInt32(&n.closed, handlerOpening, handlerClosed) {
		_ = n.pinger.Close()
		n.pinger = nil
		err := n.conn.Close()
		return err
	}
	return nil
}

func (n *nodeConn) Closed() bool {
	return atomic.LoadInt32(&n.closed) == handlerClosed
}
