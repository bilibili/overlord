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

	nodeReadBufSize = 4096
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
		br:      bufio.NewReader(conn, bufio.Get(nodeReadBufSize)),
		bw:      bufio.NewWriter(conn),
	}
}

func (nc *nodeConn) Write(m *proto.Message) (err error) {
	if nc.Closed() {
		err = errors.WithStack(ErrNodeConnClosed)
		return
	}
	req, ok := m.Request().(*Request)
	if !ok {
		err = errors.WithStack(ErrBadAssert)
		return
	}
	if !req.IsSupport() || req.IsCtl() {
		return
	}
	m.MarkWrite()
	if err = req.resp.encode(nc.bw); err != nil {
		err = errors.WithStack(err)
	}
	return
}

func (nc *nodeConn) Flush() error {
	if nc.Closed() {
		return errors.WithStack(ErrNodeConnClosed)
	}
	return nc.bw.Flush()
}

func (nc *nodeConn) Read(m *proto.Message) (err error) {
	if nc.Closed() {
		err = errors.WithStack(ErrNodeConnClosed)
		return
	}
	req, ok := m.Request().(*Request)
	if !ok {
		err = errors.WithStack(ErrBadAssert)
		return
	}
	if !req.IsSupport() || req.IsCtl() {
		return
	}
	for {
		if err = req.reply.decode(nc.br); err == bufio.ErrBufferFull {
			if err = nc.br.Read(); err != nil {
				err = errors.WithStack(err)
				return
			}
			continue
		} else if err != nil {
			err = errors.WithStack(err)
			return
		}
		m.MarkRead()
		return
	}
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
