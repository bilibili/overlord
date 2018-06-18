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

	"github.com/felixhao/overlord/lib/conv"
	"github.com/felixhao/overlord/lib/hashkit"
	"github.com/felixhao/overlord/lib/log"
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
	ping   proto.Pinger
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

func (c *channel) push(req *proto.Message) {
	i := atomic.AddInt32(&c.idx, 1)
	c.chs[i%c.cnt] <- req
}

// Cluster is cache cluster.
type Cluster struct {
	cc     *ClusterConfig
	ctx    context.Context
	cancel context.CancelFunc

	hashTag []byte

	ring  *hashkit.HashRing
	alias bool
	// nodeConn  map[string]proto.NodeConn
	nodeAlias map[string]string
	// nodePing  map[string]*pinger
	nodeCh map[string]*channel

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
	// nm := map[string]proto.NodeConn{}
	am := map[string]string{}
	// pm := map[string]*pinger{}
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
		// nm[node] = newNodeConn(cc, addrs[i])
		// pm[node] = &pinger{ping: newPinger(cc, addrs[i]), node: node, weight: ws[i]}

	}
	c.ring = ring
	c.alias = alias
	// c.nodeConn = nm
	c.nodeAlias = am
	// c.nodePing = pm
	// for node := range c.nodeConn {
	// 	rc := newChannel(cc.NodeConnections)
	// 	cm[node] = rc
	// 	go c.process(node, rc)
	// }
	c.nodeCh = cm
	// auto eject
	// if cc.PingAutoEject {
	// 	c.keepAlive()
	// }
	return
}

// Dispatch dispatchs Msg.
func (c *Cluster) Dispatch(m *proto.Message) {
	// hash
	node, ok := c.hash(m.Request().Key())
	if !ok {
		if log.V(3) {
			// log.Warnf("cluster(%s) addr(%s) Msg(%s) hash node not ok", c.cc.Name, c.cc.ListenAddr, req.Key())
		}
		m.DoneWithError(errors.Wrap(ErrClusterHashNoNode, "Cluster Dispatch dispatch Msg hash"))
		return
	}
	rc, ok := c.nodeCh[node]
	if !ok {
		if log.V(3) {
			// log.Warnf("cluster(%s) addr(%s) Msg(%s) node(%s) have not Chan", c.cc.Name, c.cc.ListenAddr, req.Key(), node)
		}
		m.DoneWithError(errors.Wrap(ErrClusterHashNoNode, "Cluster Dispatch dispatch Msg node chan"))
		return
	}
	rc.push(m)
}

func (c *Cluster) process(rc *channel, addr string) {
	for i := int32(0); i < rc.cnt; i++ {
		go func(i int32) {
			ch := rc.chs[i]
			w := newNodeConn(c.cc, addr)
			mCh := make(chan *proto.Message, 1024)
			go c.processWrite(ch, w, mCh)
			go c.processRead(w, mCh)
		}(i)
	}
}

func (c *Cluster) processWrite(ch <-chan *proto.Message, w proto.NodeConn, mCh chan<- *proto.Message) {
	for {
		var m *proto.Message
		select {
		case m = <-ch:
		case <-c.ctx.Done():
			w.Close()
			return
		default:
			// TODO(felix): multi
		}
		// now := time.Now()
		err := w.Write(m)
		// stat.HandleTime(c.cc.Name, node, req.Cmd(), int64(time.Since(now)/time.Microsecond))
		if err != nil {
			m.DoneWithError(errors.Wrap(err, "Cluster process handle"))
			if log.V(1) {
				// log.Errorf("cluster(%s) addr(%s) Msg(%s) cluster process handle error:%+v", c.cc.Name, c.cc.ListenAddr, req.Key(), err)
			}
			// stat.ErrIncr(c.cc.Name, node, req.Cmd(), errors.Cause(err).Error())
			continue
		}
		mCh <- m
	}
}

func (c *Cluster) processRead(r proto.NodeConn, mCh <-chan *proto.Message) {
	for {
		m := <-mCh
		m.ResetBuffer()
		err := r.Read(m)
		if err != nil {
			m.DoneWithError(errors.Wrap(err, "Cluster process handle"))
			if log.V(1) {
				// log.Errorf("cluster(%s) addr(%s) Msg(%s) cluster process handle error:%+v", c.cc.Name, c.cc.ListenAddr, req.Key(), err)
			}
			// stat.ErrIncr(c.cc.Name, node, req.Cmd(), errors.Cause(err).Error())
			continue
		}
		m.Done()
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
	// for _, p := range c.nodePing {
	// 	p.ping.Close()
	// }
	// for _, p := range c.nodeConn {
	// 	p.Close()
	// }
	return nil
}

func (c *Cluster) keepAlive() {
	// var period = func(p *pinger) {
	// 	del := false
	// 	for {
	// 		if err := p.ping.Ping(); err != nil {
	// 			p.failure++
	// 			p.retries = 0
	// 		} else {
	// 			p.failure = 0
	// 			if del {
	// 				c.ring.AddNode(p.node, p.weight)
	// 				del = false
	// 			}
	// 		}
	// 		if c.cc.PingAutoEject && p.failure >= c.cc.PingFailLimit {
	// 			c.ring.DelNode(p.node)
	// 			del = true
	// 		}
	// 		select {
	// 		case <-time.After(backoff.Backoff(p.retries)):
	// 			p.retries++
	// 			continue
	// 		case <-c.ctx.Done():
	// 			return
	// 		}
	// 	}
	// }
	// keepalive
	// for _, p := range c.nodePing {
	// 	go period(p)
	// }
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

// func newPinger(cc *ClusterConfig, addr string) proto.Pinger {
// 	dto := time.Duration(cc.DialTimeout) * time.Millisecond
// 	rto := time.Duration(cc.ReadTimeout) * time.Millisecond
// 	wto := time.Duration(cc.WriteTimeout) * time.Millisecond
// 	switch cc.CacheType {
// 	case proto.CacheTypeMemcache:
// 		// return memcache.NewPinger(addr, dto, rto, wto)
// 	case proto.CacheTypeRedis:
// 		// TODO(felix): support redis
// 	default:
// 		panic(proto.ErrNoSupportCacheType)
// 	}
// 	return nil
// }
