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

	"overlord/lib/backoff"
	"overlord/lib/conv"
	"overlord/lib/hashkit"
	"overlord/lib/log"
	"overlord/proto"
	"overlord/proto/memcache"
	mcbin "overlord/proto/memcache/binary"
	"overlord/proto/redis"

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
	weight []int

	failure int
	retries int
}

type batchChanel struct {
	idx int32
	cnt int32
	chs []chan *proto.MsgBatch
}

func newBatchChanel(n int32) *batchChanel {
	chs := make([]chan *proto.MsgBatch, n)
	for i := int32(0); i < n; i++ {
		chs[i] = make(chan *proto.MsgBatch, 1)
	}
	return &batchChanel{cnt: n, chs: chs}
}

func (c *batchChanel) push(m *proto.MsgBatch) {
	i := atomic.AddInt32(&c.idx, 1)
	c.chs[i%c.cnt] <- m
}

// Cluster is cache cluster.
type Cluster struct {
	cc     *ClusterConfig
	ctx    context.Context
	cancel context.CancelFunc

	hashTag []byte

	ring *hashkit.HashRing

	alias    bool
	nodeMap  map[string]int
	aliasMap map[string]int
	nodeChan map[int]*batchChanel

	syncConn proto.NodeConn

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
	c.alias = alias

	var wlist [][]int
	ring := hashkit.NewRing(cc.HashDistribution, cc.HashMethod)
	if cc.CacheType == proto.CacheTypeMemcache || cc.CacheType == proto.CacheTypeMemcacheBinary || cc.CacheType == proto.CacheTypeRedis {
		if c.alias {
			ring.Init(ans, ws)
		} else {
			ring.Init(addrs, ws)
		}
		wlist = make([][]int, len(ws))
		for i, w := range ws {
			wlist[i] = []int{w}
		}
	} else {
		panic("unsupported protocol")
	}

	nodeChan := make(map[int]*batchChanel)
	nodeMap := make(map[string]int)
	aliasMap := make(map[string]int)

	for i := range addrs {
		nbc := newBatchChanel(cc.NodeConnections)
		go c.processBatch(nbc, addrs[i])
		nodeChan[i] = nbc
		nodeMap[addrs[i]] = i
		if c.alias {
			aliasMap[ans[i]] = i
		}
	}

	c.aliasMap = aliasMap
	c.nodeChan = nodeChan
	c.nodeMap = nodeMap
	c.ring = ring
	if c.cc.PingAutoEject {
		go c.startPinger(c.cc, addrs, wlist)
	}
	return
}

func (c *Cluster) calculateBatchIndex(key []byte) int {
	node, ok := c.hash(key)
	if !ok {
		if log.V(3) {
			log.Warnf("cluster(%s) addr(%s) Msg(%s) hash node not ok", c.cc.Name, c.cc.ListenAddr, key)
		}
		return -1
	}
	if c.alias {
		return c.aliasMap[node]
	}
	return c.nodeMap[node]
}

// DispatchBatch delivers all the messages to batch execute by hash
func (c *Cluster) DispatchBatch(mbs []*proto.MsgBatch, slice []*proto.Message) {
	// TODO: dynamic update mbs by add more than configrured nodes
	var (
		bidx int
	)

	for _, msg := range slice {
		if msg.IsBatch() {
			for _, sub := range msg.Batch() {
				bidx = c.calculateBatchIndex(sub.Request().Key())
				mbs[bidx].AddMsg(sub)
			}
		} else {
			bidx = c.calculateBatchIndex(msg.Request().Key())
			mbs[bidx].AddMsg(msg)
		}
	}

	c.deliver(mbs)
}

func (c *Cluster) deliver(mbs []*proto.MsgBatch) {
	for i := range mbs {
		if mbs[i].Count() != 0 {
			mbs[i].Add(1)
			c.nodeChan[i].push(mbs[i])
		}
	}
}

func (c *Cluster) processBatch(nbc *batchChanel, addr string) {
	for i := int32(0); i < nbc.cnt; i++ {
		go func(i int32) {
			ch := nbc.chs[i]
			w := newNodeConn(c.cc, addr)
			c.processBatchIO(addr, ch, w)
		}(i)
	}
}

func (c *Cluster) processBatchIO(addr string, ch <-chan *proto.MsgBatch, nc proto.NodeConn) {
	for {
		var mb *proto.MsgBatch
		select {
		case mb = <-ch:
		case <-c.ctx.Done():
			err := nc.Close()
			if err != nil {
				if log.V(1) {
					log.Errorf("Cluster(%s) addr(%s) cancel with context", c.cc.Name, addr)
				}
			}
			return
		}

		err := c.processWriteBatch(nc, mb)
		if err != nil {
			err = errors.Wrap(err, "Cluster batch write")
			mb.BatchDoneWithError(c.cc.Name, addr, err)
			continue
		}

		err = c.processReadBatch(nc, mb)
		if err != nil {
			err = errors.Wrap(err, "Cluster batch read")
			mb.BatchDoneWithError(c.cc.Name, addr, err)
			continue
		}
		mb.BatchDone(c.cc.Name, addr)
	}
}

func (c *Cluster) processWriteBatch(w proto.NodeConn, mb *proto.MsgBatch) error {
	return w.WriteBatch(mb)
}

func (c *Cluster) processReadBatch(r proto.NodeConn, mb *proto.MsgBatch) error {
	err := r.ReadBatch(mb)
	return err
}

func (c *Cluster) startPinger(cc *ClusterConfig, addrs []string, ws [][]int) {
	for idx, addr := range addrs {
		w := ws[idx]
		nc := newNodeConn(cc, addr)
		p := &pinger{ping: nc, node: addr, weight: w}
		go c.processPing(p)
	}
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
			log.Warnf("node ping fail:%d times with err:%v", p.failure, err)
		} else {
			p.failure = 0
			if del {
				c.ring.AddNode(p.node, p.weight...)
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
	node, ok = c.ring.GetNode(realKey)
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
	case proto.CacheTypeMemcacheBinary:
		return mcbin.NewNodeConn(cc.Name, addr, dto, rto, wto)
	case proto.CacheTypeRedis:
		return redis.NewNodeConn(cc.Name, addr, dto, rto, wto)
	default:
		panic(proto.ErrNoSupportCacheType)
	}
}
