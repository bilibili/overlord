package cluster

import (
	"bytes"
	"strings"
	"sync"
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
)

type nodeConn struct {
	c  *cluster
	nc proto.NodeConn

	mnc   map[string]proto.NodeConn
	mba   *proto.MsgBatchAllocator
	sb    strings.Builder
	mLock sync.Mutex

	state int32
}

func newNodeConn(nc proto.NodeConn) proto.NodeConn {
	return &nodeConn{
		nc:  nc,
		mba: proto.GetMsgBatchAllocator(),
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
		reply := req.Reply()
		if reply.Type() != respRedirect {
			continue
		}
		data := reply.Data()
		if !bytes.HasPrefix(data, askBytes) && !bytes.HasPrefix(data, movedBytes) {
			continue
		}
		addrBs, slot, isAsk, err := parseRedirect(data)
		nc.mLock.Lock()
		nc.sb.Reset()
		nc.sb.Write(addrBs)
		addr := nc.sb.String()
		nc.mba.AddMsg(addr, m)
		nc.mLock.Unlock()
	}
	nc.redirectProcess()
	return
}

func (nc *nodeConn) redirectProcess() {
	nc.mLock.Lock()
	for addr, mb := range nc.mba.MsgBatchs() {
		if mb.Count() == 0 {
			continue
		}
		rnc, ok := nc.mnc[addr]
		if !ok {
			rnc = redis.NewNodeConn(nc.c.name, addr, nc.c.dto, nc.c.rto, nc.c.wto)
			nc.mnc[addr] = rnc
		}

		rnc.WriteBatch(mb)
		rnc.ReadBatch(mb)
	}

	nc.mLock.Unlock()
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
