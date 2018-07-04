package memcache

import (
	"bytes"
	"io"
	"sync/atomic"
	"time"

	"github.com/felixhao/overlord/lib/bufio"
	libnet "github.com/felixhao/overlord/lib/net"
	"github.com/felixhao/overlord/lib/prom"
	"github.com/felixhao/overlord/proto"
	"github.com/pkg/errors"
)

const (
	handlerOpening     = int32(0)
	handlerClosed      = int32(1)
	defaultRespBufSize = 1024 * 1024 // 1MB buffer
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
		br:      bufio.NewReader(conn, bufio.Get(defaultRespBufSize)),
		pinger:  newMCPinger(conn.Dup()),
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
	if n.Closed() {
		err = errors.Wrap(ErrClosed, "MC Handler handle Msg")
		return
	}
	for _, m := range mb.Msgs() {
		err = n.write(m)
		if err != nil {
			m.DoneWithError(err)
			return err
		}
		m.MarkWrite()
	}
	return
}

func (n *nodeConn) Flush() error {
	return n.bw.Flush()
}

func (n *nodeConn) write(m *proto.Message) (err error) {
	mcr, ok := m.Request().(*MCRequest)
	if !ok {
		err = errors.Wrap(ErrAssertMsg, "MC Handler handle assert MCMsg")
		return
	}
	_ = n.bw.WriteString(mcr.rTp.String())
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

func (n *nodeConn) ReadMBatch(mbs []*proto.MsgBatch) (err error) {
	defer n.br.Buffer().Reset()
	var (
		mcr    *MCRequest
		ok     = false
		cursor int
		size   int
		msgs   = proto.Flatten(mbs)
		marks  = make([]int, len(msgs))
	)

	idx := 0
	for {
		err = n.br.Read()
		if err != nil {
			return
		}

		for {
			if idx >= len(msgs) {
				goto cpybuf
			}
			mcr, ok = msgs[idx].Request().(*MCRequest)
			if !ok {
				err = ErrAssertMsg
				return
			}
			size, err = n.fillMCRequest(mcr, n.br.Buffer().Bytes()[cursor:])
			if err == bufio.ErrBufferFull {
				break
			} else if err != nil {
				return err
			}
			cursor += size
			marks[idx] = size
			idx++
		}
	}

cpybuf:
	n.copyToBuffer(marks, mbs)
	n.fullFillMsgs(marks, mbs, msgs)
	return
}

func (n *nodeConn) copyToBuffer(marks []int, mbs []*proto.MsgBatch) {
	var last int
	for _, mb := range mbs {
		rg := sum(marks[last : mb.Count()+last])
		last += mb.Count()
		_ = n.br.CopyTo(mb.Buffer(), rg)
	}
}

func (n *nodeConn) fullFillMsgs(marks []int, mbs []*proto.MsgBatch, msgs []*proto.Message) {
	var (
		last int
	)
	for _, mb := range mbs {
		count := mb.Count()
		cursor := 0
		for i := last; i < last+count; i++ {
			mcr, _ := msgs[i].Request().(*MCRequest)
			mcr.data = mb.Buffer().Bytes()[cursor : cursor+marks[i]]
			cursor += marks[i]
		}
		last += count
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
	prom.Hit(n.cluster, n.addr)

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

func sum(vals []int) int {
	var sum int
	for _, v := range vals {
		sum += v
	}
	return sum
}
