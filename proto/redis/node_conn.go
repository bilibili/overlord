package redis

import (
	errs "errors"
	"sync/atomic"
	"time"

	"overlord/lib/bufio"
	libnet "overlord/lib/net"
	"overlord/proto"

	"github.com/pkg/errors"
)

const (
	opened = int32(0)
	closed = int32(1)
)

var (
	// ErrNodeConnClosed err node conn closed.
	ErrNodeConnClosed = errs.New("redis node conn closed")
)

// NodeConn is export type by nodeConn for redis-cluster.
type NodeConn = nodeConn

// Bw return bufio.Writer.
func (nc *NodeConn) Bw() *bufio.Writer {
	return nc.bw
}

type nodeConn struct {
	cluster string
	addr    string
	conn    *libnet.Conn
	bw      *bufio.Writer
	br      *bufio.Reader

	state int32
}

// NewNodeConn create the node conn from proxy to redis
func NewNodeConn(cluster, addr string, dialTimeout, readTimeout, writeTimeout time.Duration) (nc proto.NodeConn) {
	conn := libnet.DialWithTimeout(addr, dialTimeout, readTimeout, writeTimeout)
	return newNodeConn(cluster, addr, conn)
}

func newNodeConn(cluster, addr string, conn *libnet.Conn) proto.NodeConn {
	return &nodeConn{
		cluster: cluster,
		addr:    addr,
		conn:    conn,
		br:      bufio.NewReader(conn, bufio.Get(4096)),
		bw:      bufio.NewWriter(conn),
	}
}

func (nc *nodeConn) WriteBatch(mb *proto.MsgBatch) (err error) {
	if nc.Closed() {
		err = errors.Wrap(ErrNodeConnClosed, "Redis Reader read batch message")
		return
	}
	for _, m := range mb.Msgs() {
		req, ok := m.Request().(*Request)
		if !ok {
			m.WithError(ErrBadAssert)
			return ErrBadAssert
		}
		if !req.IsSupport() || req.IsCtl() {
			continue
		}
		if err = req.resp.encode(nc.bw); err != nil {
			m.WithError(err)
			return err
		}
		m.MarkWrite()
	}
	return
}

func (nc *nodeConn) Flush() error {
	return nc.bw.Flush()
}

func (nc *nodeConn) ReadBatch(mb *proto.MsgBatch) (err error) {
	if nc.Closed() {
		err = errors.Wrap(ErrNodeConnClosed, "Redis Reader read batch message")
		return
	}
	// nc.br.ResetBuffer(mb.Buffer())
	// defer nc.br.ResetBuffer(nil)
	for i := 0; i < mb.Count(); {
		m := mb.Nth(i)
		req, ok := m.Request().(*Request)
		if !ok {
			return ErrBadAssert
		}
		if !req.IsSupport() || req.IsCtl() {
			i++
			continue
		}
		if err = req.reply.decode(nc.br); err == bufio.ErrBufferFull {
			if err = nc.br.Read(); err != nil {
				return
			}
			continue
		} else if err != nil {
			return
		}
		i++
	}
	return
}

func (nc *nodeConn) Close() (err error) {
	if atomic.CompareAndSwapInt32(&nc.state, opened, closed) {
		return nc.conn.Close()
	}
	return
}

func (nc *nodeConn) Closed() bool {
	return atomic.LoadInt32(&nc.state) == closed
}
