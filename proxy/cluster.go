package proxy

import (
	"bytes"
	"context"
	errs "errors"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/felixhao/overlord/lib/backoff"
	"github.com/felixhao/overlord/lib/conv"
	"github.com/felixhao/overlord/lib/hashkit"
	"github.com/felixhao/overlord/lib/log"
	"github.com/felixhao/overlord/lib/prom"
	"github.com/felixhao/overlord/proto"
	"github.com/felixhao/overlord/proto/memcache"
	"github.com/pkg/errors"
)

// cluster errors
var (
	ErrClusterServerFormat = errs.New("cluster servers format error")
	ErrClusterHashNoNode   = errs.New("cluster hash no hit node")
)

type pinger struct {
	ping   proto.NodeConn
	node   string
	weight int

	failure int
	retries int
}

type channel struct {
	idx int32
	cnt int32
	chs []chan *proto.Message
}

func newChannel(n int32) *channel {
	chs := make([]chan *proto.Message, n)
	for i := int32(0); i < n; i++ {
		chs[i] = make(chan *proto.Message, 1)
	}
	return &channel{cnt: n, chs: chs}
}

func (c *channel) push(m *proto.Message) {
	i := atomic.AddInt32(&c.idx, 1)
	c.chs[i%c.cnt] <- m
}

// Cluster is cache cluster.
type Cluster struct {
	cc     *ClusterConfig
	ctx    context.Context
	cancel context.CancelFunc

	hashTag []byte

	ring      *hashkit.HashRing
	alias     bool
	nodeAlias map[string]string
	nodeCh    map[string]*channel

	lock   sync.Mutex
	closed bool
}

// NewCluster new a cluster by cluster config.
func NewCluster(ctx context.Context, cc *ClusterConfig) (c *Cluster) {
	c = &Cluster{cc: cc}
	c.ctx, c.cancel = context.WithCancel(ctx)
	// parse
	addrs, ws, ans, alias, err := parseServers(cc.Servers)
	if err != nil {
		panic(err)
	}
	if len(cc.HashTag) == 2 {
		c.hashTag = []byte{cc.HashTag[0], cc.HashTag[1]}
	}
	ring := hashkit.Ketama()
	if alias {
		ring.Init(ans, ws)
	} else {
		ring.Init(addrs, ws)
	}
	am := map[string]string{}
	cm := map[string]*channel{}
	// for addrs
	for i := range addrs {
		node := addrs[i]
		if alias {
			node = ans[i]
			am[ans[i]] = addrs[i]
		}
		rc := newChannel(cc.NodeConnections)
		cm[node] = rc
		go c.process(rc, addrs[i])

	}
	c.ring = ring
	c.alias = alias
	c.nodeAlias = am
	c.nodeCh = cm
	if c.cc.PingAutoEject {
		go c.startPinger(c.cc, addrs, ws)
	}
	return
}

// Dispatch dispatchs Msg.
func (c *Cluster) Dispatch(m *proto.Message) {
	// hash
	node, ok := c.hash(m.Request().Key())
	if !ok {
		if log.V(3) {
			log.Warnf("cluster(%s) addr(%s) Msg(%s) hash node not ok", c.cc.Name, c.cc.ListenAddr, m.Request().Key())
		}
		m.DoneWithError(errors.Wrap(ErrClusterHashNoNode, "Cluster Dispatch dispatch Msg hash"))
		return
	}
	rc, ok := c.nodeCh[node]
	if !ok {
		if log.V(3) {
			log.Warnf("cluster(%s) addr(%s) Msg(%s) node(%s) have not Chan", c.cc.Name, c.cc.ListenAddr, m.Request().Key(), node)
		}
		m.DoneWithError(errors.Wrap(ErrClusterHashNoNode, "Cluster Dispatch dispatch Msg node chan"))
		return
	}
	rc.push(m)
}

func (c *Cluster) startPinger(cc *ClusterConfig, addrs []string, ws []int) {
	for idx, addr := range addrs {
		w := ws[idx]
		nc := newNodeConn(cc, addr)
		p := &pinger{ping: nc, node: addr, weight: w}
		go c.processPing(p)
	}
}

func (c *Cluster) process(rc *channel, addr string) {
	for i := int32(0); i < rc.cnt; i++ {
		go func(i int32) {
			ch := rc.chs[i]
			w := newNodeConn(c.cc, addr)
			c.processIO(addr, ch, w)
		}(i)
	}
}

func (c *Cluster) processIO(addr string, ch <-chan *proto.Message, nc proto.NodeConn) {
	for {
		var m *proto.Message
		select {
		case m = <-ch:
		case <-c.ctx.Done():
			nc.Close()
			return
		}

		err := c.processWrite(nc, m)
		if err != nil {
			m.DoneWithError(errors.Wrap(err, "Cluster process handle"))
			if log.V(1) {
				log.Errorf("cluster(%s) addr(%s) Msg(%s) cluster process handle error:%+v", c.cc.Name, c.cc.ListenAddr, m.Request().Key(), err)
			}
			prom.ErrIncr(c.cc.Name, addr, m.Request().Cmd(), errors.Cause(err).Error())
			continue
		}
		err = c.processRead(nc, m)
		if err != nil {
			m.DoneWithError(errors.Wrap(err, "Cluster process handle"))
			if log.V(1) {
				log.Errorf("cluster(%s) addr(%s) Msg(%s) cluster process handle error:%+v", c.cc.Name, c.cc.ListenAddr, m.Request().Key(), err)
			}
			prom.ErrIncr(c.cc.Name, addr, m.Request().Cmd(), errors.Cause(err).Error())
			continue
		}
		prom.HandleTime(c.cc.Name, addr, m.Request().Cmd(), int64(m.RemoteDur()/time.Microsecond))
		m.Done()
	}
}

func (c *Cluster) processWrite(w proto.NodeConn, m *proto.Message) error {
	m.MarkWrite()
	return w.Write(m)
}

func (c *Cluster) processRead(r proto.NodeConn, m *proto.Message) error {
	err := r.Read(m)
	m.MarkRead()
	return err
}

func (c *Cluster) processPing(p *pinger) {
	del := false
	for {
		select {
		case <-c.ctx.Done():
			return
		default:
		}
		if err := p.ping.Ping(); err != nil {
			p.failure++
			p.retries = 0
		} else {
			p.failure = 0
			if del {
				c.ring.AddNode(p.node, p.weight)
				del = false
			}
		}
		if c.cc.PingAutoEject && p.failure >= c.cc.PingFailLimit {
			c.ring.DelNode(p.node)
			del = true
		}
		select {
		case <-time.After(backoff.Backoff(p.retries)):
			p.retries++
			continue
		case <-c.ctx.Done():
			return
		}
	}
}

// hash returns node by hash hit.
func (c *Cluster) hash(key []byte) (node string, ok bool) {
	var realKey []byte
	if len(c.hashTag) == 2 {
		if b := bytes.IndexByte(key, c.hashTag[0]); b >= 0 {
			if e := bytes.IndexByte(key[b+1:], c.hashTag[1]); e >= 0 {
				realKey = key[b+1 : b+1+e]
			}
		}
	}
	if len(realKey) == 0 {
		realKey = key
	}
	node, ok = c.ring.Hash(realKey)
	return
}

// Close closes resources.
func (c *Cluster) Close() error {
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.closed {
		return nil
	}
	c.closed = true
	return nil
}

func parseServers(svrs []string) (addrs []string, ws []int, ans []string, alias bool, err error) {
	for _, svr := range svrs {
		if strings.Contains(svr, " ") {
			alias = true
		} else if alias {
			err = ErrClusterServerFormat
			return
		}
		var (
			ss    []string
			addrW string
		)
		if alias {
			ss = strings.Split(svr, " ")
			if len(ss) != 2 {
				err = ErrClusterServerFormat
				return
			}
			addrW = ss[0]
			ans = append(ans, ss[1])
		} else {
			addrW = svr
		}
		ss = strings.Split(addrW, ":")
		if len(ss) != 3 {
			err = ErrClusterServerFormat
			return
		}
		addrs = append(addrs, net.JoinHostPort(ss[0], ss[1]))
		w, we := conv.Btoi([]byte(ss[2]))
		if we != nil || w <= 0 {
			err = ErrClusterServerFormat
			return
		}
		ws = append(ws, int(w))
	}
	return
}

func newNodeConn(cc *ClusterConfig, addr string) proto.NodeConn {
	dto := time.Duration(cc.DialTimeout) * time.Millisecond
	rto := time.Duration(cc.ReadTimeout) * time.Millisecond
	wto := time.Duration(cc.WriteTimeout) * time.Millisecond
	switch cc.CacheType {
	case proto.CacheTypeMemcache:
		return memcache.NewNodeConn(cc.Name, addr, dto, rto, wto)
	case proto.CacheTypeRedis:
	// TODO(felix): support redis
	default:
		panic(proto.ErrNoSupportCacheType)
	}
	return nil
}
