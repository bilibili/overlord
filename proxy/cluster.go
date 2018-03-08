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
	"github.com/felixhao/overlord/lib/ketama"
	"github.com/felixhao/overlord/lib/log"
	"github.com/felixhao/overlord/lib/pool"
	"github.com/felixhao/overlord/lib/stat"
	"github.com/felixhao/overlord/proto"
	"github.com/felixhao/overlord/proto/memcache"
	"github.com/pkg/errors"
)

const (
	hashRingSpots     = 255
	channelNum        = 10
	channelRoutineNum = channelNum * 10
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
	chs []*proto.RequestChan
}

func newChannel(n int32) *channel {
	chs := make([]*proto.RequestChan, n)
	for i := int32(0); i < n; i++ {
		chs[i] = proto.NewRequestChan()
	}
	return &channel{cnt: n, chs: chs}
}

func (c *channel) push(req *proto.Request) {
	i := atomic.AddInt32(&c.idx, 1)
	c.chs[i%c.cnt].PushBack(req)
}

// Cluster is cache cluster.
type Cluster struct {
	cc     *ClusterConfig
	ctx    context.Context
	cancel context.CancelFunc

	hashTag []byte

	ring      *ketama.HashRing
	alias     bool
	nodePool  map[string]*pool.Pool
	nodeAlias map[string]string
	nodePing  map[string]*pinger
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
	ring := ketama.NewRing(hashRingSpots)
	if alias {
		ring.Init(ans, ws)
	} else {
		ring.Init(addrs, ws)
	}
	nm := map[string]*pool.Pool{}
	am := map[string]string{}
	pm := map[string]*pinger{}
	cm := map[string]*channel{}
	// for addrs
	for i := range addrs {
		node := addrs[i]
		if alias {
			node = ans[i]
			am[ans[i]] = addrs[i]
		}
		nm[node] = newPool(cc, addrs[i])
		pm[node] = &pinger{ping: newPinger(cc, addrs[i]), node: node, weight: ws[i]}
		rc := newChannel(channelNum)
		cm[node] = rc
		for i := 0; i < cc.PoolActive; i++ {
			go c.process(node, rc)
		}
	}
	c.ring = ring
	c.alias = alias
	c.nodePool = nm
	c.nodeAlias = am
	c.nodePing = pm
	c.nodeCh = cm
	// auto eject
	if cc.PingAutoEject {
		go c.keepAlive()
	}
	return
}

// Dispatch dispatchs request.
func (c *Cluster) Dispatch(req *proto.Request) {
	// hash
	node, ok := c.hash(req.Key())
	if !ok {
		if log.V(3) {
			log.Warnf("cluster(%s) addr(%s) request(%s) hash node not ok", c.cc.Name, c.cc.ListenAddr, req.Key())
		}
		req.DoneWithError(errors.Wrap(ErrClusterHashNoNode, "Cluster Dispatch dispatch request hash"))
		return
	}
	rc, ok := c.nodeCh[node]
	if !ok {
		if log.V(3) {
			log.Warnf("cluster(%s) addr(%s) request(%s) node(%s) have not Chan", c.cc.Name, c.cc.ListenAddr, req.Key(), node)
		}
		req.DoneWithError(errors.Wrap(ErrClusterHashNoNode, "Cluster Dispatch dispatch request node chan"))
		return
	}
	rc.push(req)
}

func (c *Cluster) process(node string, rc *channel) {
	for i := int32(0); i < rc.cnt; i++ {
		go func(i int32) {
			ch := rc.chs[i]
			for {
				var (
					reqs *proto.Request
					ok   bool
				)
				reqs, ok = ch.PopAll()
				if !ok {
					return
				}
				hdl, err := c.get(node)
				if err != nil {
					for req := reqs; req != nil; req = req.Next {
						req.DoneWithError(errors.Wrap(err, "Cluster process get handler"))
						if log.V(1) {
							log.Errorf("cluster(%s) addr(%s) cluster process init error:%+v", c.cc.Name, c.cc.ListenAddr, err)
						}
					}
					return
				}
				resp, err := hdl.Handle(reqs)
				c.put(node, hdl, err)
				if err != nil {
					for j, req := 0, reqs; req != nil; req, j = req.Next, j+1 {
						req.DoneWithError(errors.Wrap(err, "Cluster process handle"))
						if log.V(1) {
							log.Errorf("cluster(%s) addr(%s) request(%s) cluster process handle error:%+v", c.cc.Name, c.cc.ListenAddr, req.Key(), err)
						}
						stat.ErrIncr(c.cc.Name, node, req.Cmd(), err.Error())
						continue
					}
				} else {
					for j, req := 0, reqs; req != nil; req, j = req.Next, j+1 {
						//stat.HandleTime(c.cc.Name, node, req.Cmd(), float64(req.Since()/time.Microsecond))
						req.Done(resp[j])
					}
				}
			}
		}(i)
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

// get returns proto handler by node name.
func (c *Cluster) get(node string) (h proto.Handler, err error) {
	p, ok := c.nodePool[node]
	if !ok {
		err = ErrClusterHashNoNode
		return
	}
	tmp := p.Get()
	if h, ok = tmp.(proto.Handler); !ok {
		err = ErrClusterHashNoNode
	}
	return
}

// put puts proto handler into pool by node name.
func (c *Cluster) put(node string, h proto.Handler, err error) {
	p, ok := c.nodePool[node]
	if !ok {
		return
	}
	conn, ok := h.(pool.Conn)
	if !ok {
		return
	}
	p.Put(conn, err != nil)
}

// Close closes resources.
func (c *Cluster) Close() error {
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.closed {
		return nil
	}
	c.closed = true
	for _, p := range c.nodePing {
		p.ping.Close()
	}
	for _, p := range c.nodePool {
		p.Close()
	}
	return nil
}

func (c *Cluster) keepAlive() {
	var period = func(p *pinger) {
		del := false
		for {
			if err := p.ping.Ping(); err != nil {
				p.failure++
				p.retries = 0
			} else {
				p.failure = 0
				if del {
					c.ring.AddNode(p.node, p.weight)
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
	// keepalive
	for _, p := range c.nodePing {
		period(p)
	}
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

func newPool(cc *ClusterConfig, addr string) *pool.Pool {
	var dial *pool.PoolOption
	dto := time.Duration(cc.DialTimeout) * time.Millisecond
	rto := time.Duration(cc.ReadTimeout) * time.Millisecond
	wto := time.Duration(cc.WriteTimeout) * time.Millisecond
	switch cc.CacheType {
	case proto.CacheTypeMemcache:
		dial = pool.PoolDial(memcache.Dial(cc.Name, addr, dto, rto, wto))
	case proto.CacheTypeRedis:
		// TODO(felix): support redis
	default:
		panic(proto.ErrNoSupportCacheType)
	}
	act := pool.PoolActive(cc.PoolActive)
	idle := pool.PoolIdle(cc.PoolIdle)
	idleTo := pool.PoolIdleTimeout(time.Duration(cc.PoolIdleTimeout) * time.Millisecond)
	wait := pool.PoolWait(cc.PoolGetWait)
	return pool.NewPool(dial, act, idle, idleTo, wait)
}

func newPinger(cc *ClusterConfig, addr string) proto.Pinger {
	dto := time.Duration(cc.DialTimeout) * time.Millisecond
	rto := time.Duration(cc.ReadTimeout) * time.Millisecond
	wto := time.Duration(cc.WriteTimeout) * time.Millisecond
	switch cc.CacheType {
	case proto.CacheTypeMemcache:
		return memcache.NewPinger(addr, dto, rto, wto)
	case proto.CacheTypeRedis:
		// TODO(felix): support redis
	default:
		panic(proto.ErrNoSupportCacheType)
	}
	return nil
}
