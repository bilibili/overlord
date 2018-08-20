package proxy

import (
	"bytes"
	errs "errors"
	"net"
	"strings"
	"sync/atomic"
	"time"

	"overlord/lib/backoff"
	"overlord/lib/conv"
	"overlord/lib/hashkit"
	"overlord/lib/log"
	libnet "overlord/lib/net"
	"overlord/proto"
	"overlord/proto/memcache"
	mcbin "overlord/proto/memcache/binary"
	"overlord/proto/redis"

	"github.com/pkg/errors"
)

const (
	forwardStateOpening = int32(0)
	forwardStateClosed  = int32(1)
)

// errors
var (
	ErrConfigServerFormat = errs.New("servers config format error")
	ErrForwardHashNoNode  = errs.New("forward hash no hit node")
	ErrForwardClosed      = errs.New("forward already closed")
)

var (
	defaultForwardCacheTypes = map[proto.CacheType]struct{}{
		proto.CacheTypeMemcache:       struct{}{},
		proto.CacheTypeMemcacheBinary: struct{}{},
		proto.CacheTypeRedis:          struct{}{},
	}
)

// NewForward new a executor by cluster config.
func NewForward(cc *ClusterConfig) (c proto.Forwarder) {
	// new executor
	if _, ok := defaultForwardCacheTypes[cc.CacheType]; ok {
		return newDefaultForward(cc)
	}
	panic("unsupported protocol")
}

// defaultExecutor implement the default hashring router and msgbatch.
type defaultForward struct {
	cc *ClusterConfig

	ring    *hashkit.HashRing
	hashTag []byte

	// recording alias to real node
	alias    bool
	aliasMap map[string]string
	nodeChan map[string]*batchChan
	nodePing map[string]*pinger

	state int32
}

// newDefaultForward must combine.
func newDefaultForward(cc *ClusterConfig) proto.Forwarder {
	f := &defaultForward{cc: cc}
	// parse servers config
	addrs, ws, ans, alias, err := parseServers(cc.Servers)
	if err != nil {
		panic(err)
	}
	f.alias = alias
	f.hashTag = []byte(cc.HashTag)
	f.ring = hashkit.NewRing(cc.HashDistribution, cc.HashMethod)
	f.aliasMap = make(map[string]string)
	if alias {
		for idx, aname := range ans {
			f.aliasMap[aname] = addrs[idx]
		}
		f.ring.Init(ans, ws)
	} else {
		f.ring.Init(addrs, ws)
	}
	// start nbc
	f.nodeChan = make(map[string]*batchChan)
	for _, addr := range addrs {
		f.nodeChan[addr] = f.process(cc, addr)
	}
	if cc.PingAutoEject {
		f.nodePing = make(map[string]*pinger)
		for idx, addr := range addrs {
			w := ws[idx]
			pc := newPingConn(cc, addr)
			p := &pinger{ping: pc, cc: cc, node: addr, weight: w}
			go f.processPing(p)
		}
	}
	return f
}

// Forward impl proto.Forwarder
func (f *defaultForward) Forward(mba *proto.MsgBatchAllocator, msgs []*proto.Message) error {
	if closed := atomic.LoadInt32(&f.state); closed == forwardStateClosed {
		return ErrForwardClosed
	}
	for _, m := range msgs {
		if m.IsBatch() {
			for _, subm := range m.Batch() {
				addr, ok := f.getAddr(subm.Request().Key())
				if !ok {
					m.WithError(ErrForwardHashNoNode)
					return ErrForwardHashNoNode
				}
				mba.AddMsg(addr, subm)
			}
		} else {
			addr, ok := f.getAddr(m.Request().Key())
			if !ok {
				m.WithError(ErrForwardHashNoNode)
				return ErrForwardHashNoNode
			}
			mba.AddMsg(addr, m)
		}
	}
	for addr, mb := range mba.MsgBatchs() {
		if mb.Count() > 0 {
			// WaitGroup add one MsgBatch!!!
			mba.Add(1) // NOTE: important!!! for wait all MsgBatch done!!!
			f.nodeChan[addr].push(mb)
		}
	}
	mba.Wait()
	return nil
}

// Close close executor.
func (f *defaultForward) Close() error {
	if !atomic.CompareAndSwapInt32(&f.state, forwardStateOpening, forwardStateClosed) {
		return nil
	}
	return nil
}

// process will start the special backend connection.
func (f *defaultForward) process(cc *ClusterConfig, addr string) *batchChan {
	conns := cc.NodeConnections
	nbc := newBatchChan(conns)
	for i := int32(0); i < conns; i++ {
		ch := nbc.get(i)
		nc := newNodeConn(cc, addr)
		go f.processIO(cc.Name, addr, ch, nc)
	}
	return nbc
}

func (f *defaultForward) processIO(cluster, addr string, ch <-chan *proto.MsgBatch, nc proto.NodeConn) {
	for {
		mb := <-ch
		if err := nc.WriteBatch(mb); err != nil {
			err = errors.Wrap(err, "Cluster batch write")
			mb.BatchDoneWithError(cluster, addr, err)
			continue
		}
		if err := nc.ReadBatch(mb); err != nil {
			err = errors.Wrap(err, "Cluster batch read")
			mb.BatchDoneWithError(cluster, addr, err)
			continue
		}
		mb.Done(cluster, addr)
	}
}

func (f *defaultForward) processPing(p *pinger) {
	del := false
	for {
		if err := p.ping.Ping(); err != nil {
			p.failure++
			p.retries = 0
			log.Warnf("node ping fail:%d times with err:%v", p.failure, err)
			if netE, ok := err.(net.Error); !ok || !netE.Temporary() {
				p.ping.Close()
				p.ping = newPingConn(p.cc, p.node)
			}
		} else {
			p.failure = 0
			if del {
				f.ring.AddNode(p.node, p.weight)
				del = false
			}
		}
		if f.cc.PingAutoEject && p.failure >= f.cc.PingFailLimit {
			f.ring.DelNode(p.node)
			del = true
		}
		select {
		case <-time.After(backoff.Backoff(p.retries)):
			p.retries++
		}
	}
}

func (f *defaultForward) getAddr(key []byte) (addr string, ok bool) {
	if addr, ok = f.ring.GetNode(f.trimHashTag(key)); !ok {
		return
	}
	if f.alias {
		addr, ok = f.aliasMap[addr]
	}
	return
}

func (f *defaultForward) trimHashTag(key []byte) []byte {
	if len(f.hashTag) != 2 {
		return key
	}
	bidx := bytes.IndexByte(key, f.hashTag[0])
	if bidx == -1 {
		return key
	}
	eidx := bytes.IndexByte(key[bidx+1:], f.hashTag[1])
	if eidx == -1 {
		return key
	}
	return key[bidx+1 : bidx+1+eidx]
}

type batchChan struct {
	idx int32
	cnt int32
	chs []chan *proto.MsgBatch
}

func newBatchChan(n int32) *batchChan {
	chs := make([]chan *proto.MsgBatch, n)
	for i := int32(0); i < n; i++ {
		chs[i] = make(chan *proto.MsgBatch, 1024)
	}
	return &batchChan{cnt: n, chs: chs}
}

func (c *batchChan) push(m *proto.MsgBatch) {
	i := atomic.AddInt32(&c.idx, 1)
	c.chs[i%c.cnt] <- m
}

func (c *batchChan) get(i int32) chan *proto.MsgBatch {
	return c.chs[i%c.cnt]
}

type pinger struct {
	cc     *ClusterConfig
	ping   proto.Pinger
	node   string
	weight int

	failure int
	retries int
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

func newPingConn(cc *ClusterConfig, addr string) proto.Pinger {
	dto := time.Duration(cc.DialTimeout) * time.Millisecond
	rto := time.Duration(cc.ReadTimeout) * time.Millisecond
	wto := time.Duration(cc.WriteTimeout) * time.Millisecond

	conn := libnet.DialWithTimeout(addr, dto, rto, wto)
	switch cc.CacheType {
	case proto.CacheTypeMemcache:
		return memcache.NewPinger(conn)
	case proto.CacheTypeMemcacheBinary:
		return mcbin.NewPinger(conn)
	case proto.CacheTypeRedis:
		return redis.NewPinger(conn)
	default:
		panic(proto.ErrNoSupportCacheType)
	}
}

func parseServers(svrs []string) (addrs []string, ws []int, ans []string, alias bool, err error) {
	for _, svr := range svrs {
		if strings.Contains(svr, " ") {
			alias = true
		} else if alias {
			err = ErrConfigServerFormat
			return
		}
		var (
			ss    []string
			addrW string
		)
		if alias {
			ss = strings.Split(svr, " ")
			if len(ss) != 2 {
				err = ErrConfigServerFormat
				return
			}
			addrW = ss[0]
			ans = append(ans, ss[1])
		} else {
			addrW = svr
		}
		ss = strings.Split(addrW, ":")
		if len(ss) != 3 {
			err = ErrConfigServerFormat
			return
		}
		addrs = append(addrs, net.JoinHostPort(ss[0], ss[1]))
		w, we := conv.Btoi([]byte(ss[2]))
		if we != nil || w <= 0 {
			err = ErrConfigServerFormat
			return
		}
		ws = append(ws, int(w))
	}
	return
}
