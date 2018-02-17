package proxy

import (
	"bytes"
	"context"
	errs "errors"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/felixhao/overlord/lib/backoff"
	"github.com/felixhao/overlord/lib/conv"
	"github.com/felixhao/overlord/lib/ketama"
	"github.com/felixhao/overlord/lib/pool"
	"github.com/felixhao/overlord/proto"
	"github.com/felixhao/overlord/proto/memcache"
)

const (
	hashRingSpots = 255
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
	// for addrs
	for i := range addrs {
		node := addrs[i]
		if alias {
			node = ans[i]
			am[ans[i]] = addrs[i]
		}
		nm[node] = newPool(cc, addrs[i])
		pm[node] = &pinger{ping: newPinger(cc, addrs[i]), node: node, weight: ws[i]}
	}
	c.ring = ring
	c.alias = alias
	c.nodePool = nm
	c.nodeAlias = am
	c.nodePing = pm
	// auto eject
	if cc.PingAutoEject {
		go c.keepAlive()
	}
	return
}

// Hash returns node by hash hit.
func (c *Cluster) Hash(key []byte) (node string, ok bool) {
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

// Get returns proto handler by node name.
func (c *Cluster) Get(node string) (h proto.Handler, err error) {
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

// Put puts proto handler into pool by node name.
func (c *Cluster) Put(node string, h proto.Handler, err error) {
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
		dial = pool.PoolDial(memcache.Dial(addr, dto, rto, wto))
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
