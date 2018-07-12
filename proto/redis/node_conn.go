package redis

import (
	libnet "overlord/lib/net"
	"overlord/proto"
	"sync/atomic"
)

const (
	closed = uint32(0)
	opened = uint32(1)
)

type nodeConn struct {
	rc    *respConn
	conn  *libnet.Conn
	p     *pinger
	state uint32
}

// NewNodeConn create the node conn from proxy to redis
func NewNodeConn(conn *libnet.Conn) proto.NodeConn {
	return &nodeConn{
		rc:    newRespConn(conn),
		conn:  conn,
		p:     newPinger(conn),
		state: closed,
	}
}

func (nc *nodeConn) WriteBatch(mb *proto.MsgBatch) error {
	for _, m := range mb.Msgs() {
		err := nc.write(m)
		if err != nil {
			m.DoneWithError(err)
			return err
		}
	}
	err := nc.rc.Flush()
	return err
}

func (nc *nodeConn) write(m *proto.Message) error {
	cmd, ok := m.Request().(*Command)
	if !ok {
		m.DoneWithError(ErrBadAssert)
		return ErrBadAssert
	}
	return nc.rc.encode(cmd.respObj)
}

func (nc *nodeConn) ReadBatch(mb *proto.MsgBatch) error {
	nc.rc.br.ResetBuffer(mb.Buffer())
	defer nc.rc.br.ResetBuffer(nil)

	count := mb.Count()
	resps, err := nc.rc.decodeCount(count)
	if err != nil {
		return err
	}
	for i, msg := range mb.Msgs() {
		cmd, ok := msg.Request().(*Command)
		if !ok {
			return ErrBadAssert
		}
		cmd.reply = resps[i]
	}
	return nil
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