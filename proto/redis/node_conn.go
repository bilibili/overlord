package redis

import (
	"sync/atomic"
	"time"

	"overlord/lib/bufio"
	libnet "overlord/lib/net"
	"overlord/proto"
)

const (
	opened = uint32(0)
	closed = uint32(1)
)

var (
	reqAskResp = &resp{
		rTp:  respArray,
		data: []byte("1"),
		array: []*resp{
			&resp{
				rTp:    respBulk,
				data:   []byte("6\r\nASKING"),
				array:  nil,
				arrayn: 0,
			},
		},
		arrayn: 1,
	}

	ignoreReply = &resp{
		data:   []byte{},
		array:  []*resp{},
		arrayn: 0,
	}
)

type nodeConn struct {
	cluster string
	addr    string
	conn    *libnet.Conn
	bw      *bufio.Writer
	br      *bufio.Reader
	state   uint32

	p *pinger
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
		br:      bufio.NewReader(conn, nil),
		bw:      bufio.NewWriter(conn),
		conn:    conn,
		p:       newPinger(conn),
	}
}

func (nc *nodeConn) WriteBatch(mb *proto.MsgBatch) (err error) {
	for _, m := range mb.Msgs() {

		req, ok := m.Request().(*Request)
		if !ok {
			m.SetError(ErrBadAssert)
			return ErrBadAssert
		}
		if !req.isSupport() || req.isCtl() {
			continue
		}
		if req.Redirect != nil && req.Redirect.IsAsk {
			if err = reqAskResp.encode(nc.bw); err != nil {
				m.SetError(err)
				return
			}
		}
		if err = req.resp.encode(nc.bw); err != nil {
			m.SetError(err)
			return err
		}
		m.MarkWrite()
	}
	return nc.bw.Flush()
}

func (nc *nodeConn) ReadBatch(mb *proto.MsgBatch) (err error) {
	nc.br.ResetBuffer(mb.Buffer())
	defer nc.br.ResetBuffer(nil)
	begin := nc.br.Mark()
	now := nc.br.Mark()
	redirectFlag := false

	for i := 0; i < mb.Count(); {
		m := mb.Nth(i)
		req, ok := m.Request().(*Request)
		if !ok {
			return ErrBadAssert
		}
		if !req.isSupport() || req.isCtl() {
			i++
			continue
		}

		// if isAsk, an ask command will be send into backend
		if req.Redirect != nil && req.Redirect.IsAsk {
			if err = ignoreReply.decode(nc.br); err == bufio.ErrBufferFull {
				nc.br.AdvanceTo(begin)
				if err = nc.br.Read(); err != nil {
					return
				}
				nc.br.AdvanceTo(now)
				continue
			} else if err != nil {
				return
			}
		}
		req.Redirect = nil

		if err = req.reply.decode(nc.br); err == bufio.ErrBufferFull {
			nc.br.AdvanceTo(begin)
			if err = nc.br.Read(); err != nil {
				return
			}
			nc.br.AdvanceTo(now)
			continue
		} else if err != nil {
			return
		}
		m.MarkRead()

		// check if this call is redirect
		if isRedirect(req.reply) {
			req.Redirect, err = parseRedirectInfo(req.reply.data)
			if err != nil {
				return
			}
			redirectFlag = true
		}
		now = nc.br.Mark()
		i++
	}

	if redirectFlag {
		err = ErrRedirect
	}
	return
}

func (nc *nodeConn) Ping() (err error) {
	return nc.p.ping()
}

func (nc *nodeConn) Close() (err error) {
	if atomic.CompareAndSwapUint32(&nc.state, opened, closed) {
		return nc.conn.Close()
	}
	return
}
