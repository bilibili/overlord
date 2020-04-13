package cluster

import (
	"bytes"
	"strings"
	"sync/atomic"

	"overlord/pkg/conv"
	"overlord/pkg/log"
	"overlord/proxy/proto"
	"overlord/proxy/proto/redis"

	"github.com/pkg/errors"
)

const (
	respRedirect = '-'
	maxRedirects = 5
)

var (
	askBytes   = []byte("ASK")
	movedBytes = []byte("MOVED")

	askingResp = []byte("*1\r\n$6\r\nASKING\r\n")
)

type nodeConn struct {
	c    *cluster
	addr string
	nc   proto.NodeConn

	sb strings.Builder

	redirects int

	state int32
}

func newNodeConn(c *cluster, addr string) (nc proto.NodeConn) {
	nc = &nodeConn{
		c:    c,
		addr: addr,
		nc:   redis.NewNodeConn(c.name, addr, c.dto, c.rto, c.wto),
	}
	return
}

func (nc *nodeConn) Addr() string {
	return nc.addr
}

func (nc *nodeConn) Cluster() string {
	return nc.c.name
}

func (nc *nodeConn) Write(m *proto.Message) (err error) {
	if err = nc.nc.Write(m); err != nil {
		err = errors.WithStack(err)
	}
	return
}

func (nc *nodeConn) Flush() error {
	return nc.nc.Flush()
}

func (nc *nodeConn) Read(m *proto.Message) (err error) {
	if err = nc.nc.Read(m); err != nil {
		err = errors.WithStack(err)
		return
	}
	req := m.Request().(*redis.Request)
	// check request
	if !req.IsSupport() || req.IsCtl() {
		return
	}
	reply := req.Reply()
	if reply.Type() != respRedirect {
		return
	}
	if nc.redirects >= maxRedirects { // NOTE: check max redirects
		if log.V(4) {
			log.Infof("Redis Cluster NodeConn key(%s) already max redirects", req.Key())
		}
		return
	}
	data := reply.Data()
	if !bytes.HasPrefix(data, askBytes) && !bytes.HasPrefix(data, movedBytes) {
		return
	}

	addrBs, _, isAsk, _ := parseRedirect(data)
	if !isAsk {
		// tryFetch when key moved
		nc.c.toFetch()
	}
	nc.sb.Reset()
	nc.sb.Write(addrBs)
	addr := nc.sb.String()
	// redirect process
	if err = nc.redirectProcess(m, req, addr, isAsk); err != nil && log.V(2) {
		log.Errorf("Redis Cluster NodeConn redirectProcess addr:%s error:%v", addr, err)
	}
	nc.redirects = 0
	return
}

func (nc *nodeConn) redirectProcess(m *proto.Message, req *redis.Request, addr string, isAsk bool) (err error) {
	// next redirect
	nc.redirects++
	if log.V(5) {
		log.Infof("Redis Cluster NodeConn key(%s) redirect count(%d)", req.Key(), nc.redirects)
	}
	// start redirect
	nnc := newNodeConn(nc.c, addr)
	tmp := nnc.(*nodeConn)
	tmp.redirects = nc.redirects // NOTE: for check max redirects
	rnc := tmp.nc.(*redis.NodeConn)
	defer nnc.Close()
	if isAsk {
		if err = rnc.Bw().Write(askingResp); err != nil {
			err = errors.WithStack(err)
			return
		}
	}
	if err = req.RESP().Encode(rnc.Bw()); err != nil {
		err = errors.WithStack(err)
		return
	}
	if err = rnc.Bw().Flush(); err != nil {
		err = errors.WithStack(err)
		return
	}
	// NOTE: even if the client waits a long time before reissuing the query, and in the meantime the cluster configuration
	// changed, the destination node will reply again with a MOVED error if the hash slot is now served by another node.
	if err = nnc.Read(m); err != nil {
		err = errors.WithStack(err)
	}
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
