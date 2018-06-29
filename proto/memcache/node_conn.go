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
	defaultRespBufSize = 4 * 1024
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
	var (
		size   int
		cursor int
		nth    int
		mnth   int
		m      *proto.Message

		mcr    *MCRequest
		ok     bool
		mbsLen = len(mbs)

		batchSize = proto.MergeBatchSize(mbs)
		divider   = make([]int, batchSize+1)
		mcrs      = make([]*MCRequest, batchSize)
		last      = 1
	)

	for {
		if mnth == mbsLen {
			break
		}

		err = n.br.Read()
		if err != nil {
			err = errors.Wrap(err, "node conn while read")
			return
		}
		// get the nth m of mnth mb
		for {
			m = mbs[mnth].Nth(nth)
			if m == nil {
				mnth++
				nth = 0
				continue
			}
			break
		}

		mcr, ok = m.Request().(*MCRequest)
		if !ok {
			err = errors.Wrap(ErrAssertMsg, "MC Handler handle assert MCMsg")
			return
		}
		mcrs[last] = mcr

		size, err = n.fillMCRequest(mcr, n.br.Buffer().Bytes()[cursor:])
		if err == bufio.ErrBufferFull {
			continue
		} else if err != nil {
			return err
		}

		cursor += size
		divider[last] = cursor
		last++
		nth++
	}

	n.fullFillMsgs(divider, mbs, mcrs)
	return
}

func (n *nodeConn) fullFillMsgs(divider []int, mbs []*proto.MsgBatch, mcrs []*MCRequest) {
	var last int
	for _, mb := range mbs {
		beg := divider[last]
		last += mb.Count()
		end := divider[last]
		_ = n.br.CopyTo(mb.Buffer(), end-beg)
	}

	var (
		mnth   = 0
		offset = 0
		data   []byte
		i      = 0
	)
	for mnth != len(mbs) {
		data = mbs[mnth].Buffer().Bytes()
		begin, end := divider[i]-offset, divider[i+1]-offset

		if begin >= len(data) {
			offset += len(data)
			mnth++
			continue
		}
		mcrs[i].data = data[begin:end]
	}

}

func (n *nodeConn) fillMCRequest(mcr *MCRequest, data []byte) (size int, err error) {
	// TODO: 是当扩容的时候,这个地方其实是有多次copy问题的
	// 应该改改形式改成不需要多次copy
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
