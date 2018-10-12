package cluster

import (
	"bytes"
	"strings"
	"sync/atomic"

	"overlord/lib/conv"
	"overlord/proto"
	"overlord/proto/redis"

	pkgerrs "github.com/pkg/errors"
)

const (
	respRedirect = '-'
)

var (
	askBytes   = []byte("ASK")
	movedBytes = []byte("MOVED")

	askingResp = []byte("*1\r\n$6\r\nASKING\r\n")
)

type nodeConn struct {
	c  *cluster
	nc proto.NodeConn

	mba *proto.MsgBatchAllocator
	sb  strings.Builder

	state int32
}

func newNodeConn(c *cluster, addr string) proto.NodeConn {
	return &nodeConn{
		c:   c,
		nc:  redis.NewNodeConn(c.name, addr, c.dto, c.rto, c.wto),
		mba: proto.GetMsgBatchAllocator(),
	}
}

func (nc *nodeConn) WriteBatch(mb *proto.MsgBatch) (err error) {
	err = nc.nc.WriteBatch(mb)
	if err != nil {
		if atomic.LoadInt32(&nc.state) == closed {
			err = pkgerrs.Wrap(err, "maybe write closed")
			return
		}
	}
	return
}

func (nc *nodeConn) ReadBatch(mb *proto.MsgBatch) (err error) {
	if err = nc.nc.ReadBatch(mb); err != nil {
		if err != nil {
			if atomic.LoadInt32(&nc.state) == closed {
				err = pkgerrs.Wrap(err, "maybe read closed")
				return
			}
		}
		return
	}
	var (
		isRe    bool
		addrAsk map[string]bool
	)
	for i := 0; i < mb.Count(); i++ {
		m := mb.Nth(i)
		req := m.Request().(*redis.Request)
		reply := req.Reply()
		if reply.Type() != respRedirect {
			continue
		}
		data := reply.Data()
		if !bytes.HasPrefix(data, askBytes) && !bytes.HasPrefix(data, movedBytes) {
			continue
		}
		addrBs, _, isAsk, _ := parseRedirect(data)
		nc.sb.Reset()
		nc.sb.Write(addrBs)
		addr := nc.sb.String()
		nc.mba.AddMsg(addr, m)
		isRe = true
		if addrAsk == nil {
			addrAsk = make(map[string]bool)
		}
		addrAsk[addr] = isAsk
	}
	if isRe {
		err = nc.redirectProcess(addrAsk)
	}
	return
}


func (nc *nodeConn) Flush() error {
	return nc.nc.Flush()
}

func (nc *nodeConn) redirectProcess(addrAsk map[string]bool) (err error) {
	for addr, mb := range nc.mba.MsgBatchs() {
		if mb.Count() == 0 {
			continue
		}
		rdt := nc.c.getRedirectNodeConn(addr)
		rdt.lock.Lock()
		if rdt.nc == nil {
			rdt.lock.Unlock()       // NOTE: unlock
			return ErrClusterClosed // FIXME(felix): when closed by closeRedirectNodeConn, how
		}
		rnc := rdt.nc
		isAsk := addrAsk[addr]
		for _, m := range mb.Msgs() {
			req, ok := m.Request().(*redis.Request)
			if !ok {
				rdt.lock.Unlock() // NOTE: unlock
				m.WithError(redis.ErrBadAssert)
				return redis.ErrBadAssert
			}
			if !req.IsSupport() || req.IsCtl() {
				continue
			}
			if isAsk {
				if err = rnc.Bw().Write(askingResp); err != nil {
					rdt.lock.Unlock() // NOTE: unlock
					m.WithError(err)
					return
				}
			}
			if err = req.RESP().Encode(rnc.Bw()); err != nil {
				rdt.lock.Unlock() // NOTE: unlock
				m.WithError(err)
				return err
			}
			m.MarkWrite()
		}
		if err = rnc.Bw().Flush(); err != nil {
			rdt.lock.Unlock() // NOTE: unlock
			return
		}
		if err = rnc.ReadBatch(mb); err != nil {
			rdt.lock.Unlock() // NOTE: unlock
			return
		}
		rdt.lock.Unlock() // NOTE: unlock
		nc.c.closeRedirectNodeConn(addr, isAsk)
	}
	nc.mba.Reset()
	return
}

func (nc *nodeConn) Close() (err error) {
	if atomic.CompareAndSwapInt32(&nc.state, opening, closed) {
		return nc.nc.Close()
	}
	return
}

func parseRedirect(data []byte) (addr []byte, slot int, isAsk bool, err error) {
	fields := bytes.Fields(data)
	if len(fields) != 3 {
		return
	}
	si, err := conv.Btoi(fields[1])
	if err != nil {
		return
	}
	addr = fields[2]
	slot = int(si)
	isAsk = bytes.Equal(askBytes, fields[0])
	return
}
