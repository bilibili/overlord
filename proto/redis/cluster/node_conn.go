package cluster

import (
	"sync/atomic"

	"overlord/proto"
	"overlord/proto/redis"
)

const (
	opened = int32(0)
	closed = int32(1)
)

type nodeConn struct {
	nc  proto.NodeConn
	mnc map[string]proto.NodeConn

	state int32
}

func newNodeConn(nc proto.NodeConn) proto.NodeConn {
	return &nodeConn{
	// cluster: cluster,
	// addr:    addr,
	// conn:    conn,
	// br:      bufio.NewReader(conn, nil),
	// bw:      bufio.NewWriter(conn),
	}
}

func (nc *nodeConn) WriteBatch(mb *proto.MsgBatch) (err error) {
	err = nc.nc.WriteBatch(mb)
	return
}

func (nc *nodeConn) ReadBatch(mb *proto.MsgBatch) (err error) {
	if err = nc.nc.ReadBatch(mb); err != nil {
		return
	}
	for i := 0; i < mb.Count(); i++ {
		m := mb.Nth(i)
		req := m.Request().(*redis.Request)
		// req.IsRedirect()
	}

	return
}

func (nc *nodeConn) Close() (err error) {
	if atomic.CompareAndSwapInt32(&nc.state, opened, closed) {
		return nc.nc.Close()
	}
	return
}
