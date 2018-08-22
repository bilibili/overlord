package cluster

import (
	"bytes"
	"strings"
	"sync/atomic"

	"overlord/lib/conv"
	"overlord/proto"
	"overlord/proto/redis"
)

const (
	opened = int32(0)
	closed = int32(1)

	respRedirect = '-'
)

var (
	askBytes   = []byte("ASK")
	movedBytes = []byte("MOVED")

	reqAskResp = &redis.RESP{
		// rTp:  respArray,
		// data: []byte("1"),
		// array: []*resp{
		// 	&resp{
		// 		rTp:    respBulk,
		// 		data:   []byte("6\r\nASKING"),
		// 		array:  nil,
		// 		arrayn: 0,
		// 	},
		// },
		// arrayn: 1,
	}
)

type nodeConn struct {
	c  *cluster
	nc proto.NodeConn

	mnc map[string]*redis.NodeConn
	mba *proto.MsgBatchAllocator
	sb  strings.Builder

	state int32
}

func newNodeConn(nc proto.NodeConn) proto.NodeConn {
	return &nodeConn{
		nc: nc,
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
	var (
		isRe     bool
		moveAddr map[string]struct{}
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
		if !isAsk {
			if moveAddr == nil {
				moveAddr = make(map[string]struct{})
			}
			moveAddr[addr] = struct{}{}
		}
		isRe = true
	}
	if isRe {
		nc.redirectProcess(moveAddr)
	}
	return
}

func (nc *nodeConn) redirectProcess(moveAddr map[string]struct{}) (err error) {
	for addr, mb := range nc.mba.MsgBatchs() {
		if mb.Count() == 0 {
			continue
		}
		rnc, ok := nc.mnc[addr]
		if !ok {
			nc.mnc[addr] = redis.NewNodeConn(nc.c.name, addr, nc.c.dto, nc.c.rto, nc.c.wto).(*redis.NodeConn)
		}
		for _, m := range mb.Msgs() {
			req, ok := m.Request().(*redis.Request)
			if !ok {
				m.WithError(redis.ErrBadAssert)
				return redis.ErrBadAssert
			}
			if !req.IsSupport() || req.IsCtl() {
				continue
			}
			if err = reqAskResp.Encode(rnc.Bw()); err != nil {
				m.WithError(err)
				return
			}
			if err = req.RESP().Encode(rnc.Bw()); err != nil {
				m.WithError(err)
				return err
			}
			m.MarkWrite()
		}
		if err = rnc.Bw().Flush(); err != nil {
			return
		}
		if err = rnc.ReadBatch(mb); err != nil {
			return
		}
		if moveAddr != nil { // NOTE: when ask finish, close addr conn.
			if _, ok := moveAddr[addr]; ok {
				rnc.Close()
				delete(nc.mnc, addr)
				select {
				case nc.c.action <- struct{}{}:
				default:
				}
			}
		}
	}
	return
}

func (nc *nodeConn) Close() (err error) {
	if atomic.CompareAndSwapInt32(&nc.state, opened, closed) {
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
