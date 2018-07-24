package redis

import (
	libnet "overlord/lib/net"
	"overlord/proto"
	"sync/atomic"
	"time"
)

const (
	closed = uint32(0)
	opened = uint32(1)
)

type nodeConn struct {
	cluster string
	addr    string
	conn    *libnet.Conn
	rc      *respConn
	p       *pinger
	state   uint32

	batchCount int
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
		rc:      newRESPConn(conn),
		conn:    conn,
		p:       newPinger(conn),
		state:   closed,
	}
}

func (nc *nodeConn) WriteBatch(mb *proto.MsgBatch) error {
	for _, m := range mb.Msgs() {
		err := nc.write(m)
		if err != nil {
			m.DoneWithError(err)
			return err
		}
		m.MarkWrite()
	}
	err := nc.rc.Flush()
	return err
}

func (nc *nodeConn) write(m *proto.Message) error {
	cmd, ok := m.Request().(*Request)
	if !ok {
		m.DoneWithError(ErrBadAssert)
		return ErrBadAssert
	}

	if cmd.rtype == reqTypeNotSupport || cmd.rtype == reqTypeCtl {
		return nil
	}

	nc.batchCount++
	return cmd.respObj.encode(nc.rc.bw)
}

func (nc *nodeConn) ReadBatch(mb *proto.MsgBatch) (err error) {
	if nc.batchCount == 0 {
		return
	}
	nc.rc.br.ResetBuffer(mb.Buffer())
	defer func() {
		nc.rc.br.ResetBuffer(nil)
		nc.batchCount = 0
	}()
	return nc.rc.decodeToMsgBatch(mb)
}

func (nc *nodeConn) Ping() error {
	return nc.p.ping()
}

func (nc *nodeConn) Close() error {
	if atomic.CompareAndSwapUint32(&nc.state, opened, closed) {
		return nc.conn.Close()
	}
	return nil
}
