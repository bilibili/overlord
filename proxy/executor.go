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
	rclstr "overlord/proto/redis/cluster"

	"github.com/pkg/errors"
)

const (
	executorStateOpening = int32(0)
	executorStateClosed  = int32(1)
)

// errors
var (
	ErrConfigServerFormat = errs.New("servers config format error")
	ErrExecutorHashNoNode = errs.New("executor hash no hit node")
	ErrExecutorClosed     = errs.New("executor already closed")
)

var (
	defaultExecuteCacheTypes = map[proto.CacheType]struct{}{
		proto.CacheTypeMemcache:       struct{}{},
		proto.CacheTypeMemcacheBinary: struct{}{},
		proto.CacheTypeRedis:          struct{}{},
	}
)

// NewExecutor new a executor by cluster config.
func NewExecutor(cc *ClusterConfig) (c proto.Executor) {
	// new executor
	if _, ok := defaultExecuteCacheTypes[cc.CacheType]; ok {
		return newDefaultExecutor(cc)
	}
	if cc.CacheType == proto.CacheTypeRedisCluster {
		dto := time.Duration(cc.DialTimeout) * time.Millisecond
		rto := time.Duration(cc.ReadTimeout) * time.Millisecond
		wto := time.Duration(cc.WriteTimeout) * time.Millisecond
		return rclstr.NewExecutor(cc.Name, cc.ListenAddr, cc.Servers, cc.NodeConnections, dto, rto, wto, []byte(cc.HashTag))
	}
	panic("unsupported protocol")
}

// defaultExecutor implement the default hashring router and msgbatch.
type defaultExecutor struct {
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

// newDefaultExecutor must combine.
func newDefaultExecutor(cc *ClusterConfig) proto.Executor {
	e := &defaultExecutor{cc: cc}
	// parse servers config
	addrs, ws, ans, alias, err := parseServers(cc.Servers)
	if err != nil {
		panic(err)
	}
	e.alias = alias
	e.hashTag = []byte(cc.HashTag)
	e.ring = hashkit.NewRing(cc.HashDistribution, cc.HashMethod)
	e.aliasMap = make(map[string]string)
	if alias {
		for idx, aname := range ans {
			e.aliasMap[aname] = addrs[idx]
		}
		e.ring.Init(ans, ws)
	} else {
		e.ring.Init(addrs, ws)
	}
	// start nbc
	e.nodeChan = make(map[string]*batchChan)
	for _, addr := range addrs {
		e.nodeChan[addr] = e.process(cc, addr)
	}
	if cc.PingAutoEject {
		e.nodePing = make(map[string]*pinger)
		for idx, addr := range addrs {
			w := ws[idx]
			pc := newPingConn(cc, addr)
			p := &pinger{ping: pc, cc: cc, node: addr, weight: w}
			if e.alias {
				p.alias = ans[idx]
			}
			go e.processPing(p)
		}
	}
	return e
}

// Execute impl proto.Executor
func (e *defaultExecutor) Execute(mba *proto.MsgBatchAllocator, msgs []*proto.Message, nodem map[string]struct{}) error {
	if closed := atomic.LoadInt32(&e.state); closed == executorStateClosed {
		return ErrExecutorClosed
	}
	for _, m := range msgs {
		if m.IsBatch() {
			for _, subm := range m.Batch() {
				addr, ok := e.getAddr(subm.Request().Key())
				if !ok {
					m.WithError(ErrExecutorHashNoNode)
					return ErrExecutorHashNoNode
				}
				mba.AddMsg(addr, subm)
				nodem[addr] = struct{}{}
			}
		} else {
			addr, ok := e.getAddr(m.Request().Key())
			if !ok {
				m.WithError(ErrExecutorHashNoNode)
				return ErrExecutorHashNoNode
			}
			mba.AddMsg(addr, m)
			nodem[addr] = struct{}{}
		}
	}
	for node := range nodem {
		mba.Add(1)
		e.nodeChan[node].push(mba.GetBatch(node))
	}
	// for addr, mb := range mba.MsgBatchs() {
	// 	if mb.Count() > 0 {
	// 		// WaitGroup add one MsgBatch!!!
	// 		mba.Add(1) // NOTE: important!!! for wait all MsgBatch done!!!
	// 		e.nodeChan[addr].push(mb)
	// 	}
	// }
	mba.Wait()
	return nil
}

// Close close executor.
func (e *defaultExecutor) Close() error {
	if !atomic.CompareAndSwapInt32(&e.state, executorStateOpening, executorStateClosed) {
		return nil
	}
	return nil
}

// process will start the special backend connection.
func (e *defaultExecutor) process(cc *ClusterConfig, addr string) *batchChan {
	conns := cc.NodeConnections
	nbc := newBatchChan(conns)
	for i := int32(0); i < conns; i++ {
		ch := nbc.ch
		nc := newNodeConn(cc, addr)
		go spwanPipe(addr, cc, ch, nc)
	}
	return nbc
}

func (e *defaultExecutor) processIO(cluster, addr string, ch <-chan *proto.MsgBatch, nc proto.NodeConn) {
	// TODO: merge msgbatch for backend
	// TODO: split write and read for backend
	// TODO: remove magic number
	var mbs = make([]*proto.MsgBatch, 64)
	var cursor = 0
	var quick = false
	var err error
	for {
		if err != nil {
			_ = nc.Close()
			nc = newNodeConn(e.cc, addr)
			err = nil
		}

		select {
		case mb := <-ch:
			mbs[cursor] = mb
			cursor++
		default:
			quick = true
		}

		if (quick && cursor > 0) || (cursor == 64) {
			quick = false
			for _, mb := range mbs[:cursor] {
				if err = nc.WriteBatch(mb); err != nil {
					err = errors.Wrap(err, "Cluster batch write")
					mb.DoneWithError(cluster, addr, err)
					goto deferReset
				}
			}

			if err = nc.Flush(); err != nil {
				err = errors.Wrap(err, "Cluster batch flush")
				for _, mb := range mbs[:cursor] {
					mb.DoneWithError(cluster, addr, err)
				}
				goto deferReset
			}

			for _, mb := range mbs[:cursor] {
				if err = nc.ReadBatch(mb); err != nil {
					err = errors.Wrap(err, "Cluster batch read")
					mb.DoneWithError(cluster, addr, err)
					continue
				}
				mb.Done(cluster, addr)
			}
		deferReset:
			cursor = 0
		} else {
			mb := <-ch
			mbs[cursor] = mb
			cursor++
		}

	}
}

func (e *defaultExecutor) processPing(p *pinger) {
	del := false
	for {
		if err := p.ping.Ping(); err != nil {
			p.failure++
			p.retries = 0
			log.Warnf("node ping fail:%d times with err:%v", p.failure, err)
			if netE, ok := err.(net.Error); !ok || !netE.Temporary() {
				_ = p.ping.Close()
				p.ping = newPingConn(p.cc, p.node)
			}
		} else {
			p.failure = 0
			if del {
				if p.alias != "" {
					e.ring.AddNode(p.alias, p.weight)
				} else {
					e.ring.AddNode(p.node, p.weight)
				}
				del = false
			}
		}
		if e.cc.PingAutoEject && p.failure >= e.cc.PingFailLimit {
			if p.alias != "" {
				e.ring.DelNode(p.alias)
			} else {
				e.ring.DelNode(p.node)
			}
			del = true
		}
		<-time.After(backoff.Backoff(p.retries))
		p.retries++
	}
}

func (e *defaultExecutor) getAddr(key []byte) (addr string, ok bool) {
	if addr, ok = e.ring.GetNode(e.trimHashTag(key)); !ok {
		return
	}
	if e.alias {
		addr, ok = e.aliasMap[addr]
	}
	return
}

func (e *defaultExecutor) trimHashTag(key []byte) []byte {
	if len(e.hashTag) != 2 {
		return key
	}
	bidx := bytes.IndexByte(key, e.hashTag[0])
	if bidx == -1 {
		return key
	}
	eidx := bytes.IndexByte(key[bidx+1:], e.hashTag[1])
	if eidx == -1 {
		return key
	}
	return key[bidx+1 : bidx+1+eidx]
}

type batchChan struct {
	ch chan *proto.MsgBatch
}

func newBatchChan(n int32) *batchChan {
	return &batchChan{ch: make(chan *proto.MsgBatch, n*1024)}
}

func (c *batchChan) push(m *proto.MsgBatch) {
	c.ch <- m
}

type pinger struct {
	cc     *ClusterConfig
	ping   proto.Pinger
	node   string
	alias  string
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

type executorDown struct {
	cc      *ClusterConfig
	addr    string
	input   <-chan *proto.MsgBatch
	forward chan<- *proto.MsgBatch
	nc      proto.NodeConn
	local   []*proto.MsgBatch
}

func (ed *executorDown) spawn() {
	var count int
	var err error
	for {
		if err != nil {
			_ = ed.nc.Close()
			ed.nc = newNodeConn(ed.cc, ed.addr)
			err = nil
		}

		select {
		case mb := <-ed.input:
			if err = ed.nc.WriteBatch(mb); err != nil {
				err = errors.Wrap(err, "Cluster batch write")
				mb.DoneWithError(ed.cc.Name, ed.addr, err)
			}
			ed.local[count] = mb
			count++
			if count < 64 {
				continue
			}
		default:
		}

		if count != 0 {
			if err = ed.nc.Flush(); err != nil {
				for _, mb := range ed.local {
					mb.DoneWithError(ed.cc.Name, ed.addr, err)
				}
				count = 0
				continue
			}
			for _, mb := range ed.local[:count] {
				ed.forward <- mb
			}
			count = 0
		} else {
			mb := <-ed.input
			if err = ed.nc.WriteBatch(mb); err != nil {
				err = errors.Wrap(err, "Cluster batch write")
				mb.DoneWithError(ed.cc.Name, ed.addr, err)
			}
			ed.local[count] = mb
			count++
		}
	}
}

type executorRecv struct {
	cluster string
	addr    string
	recv    <-chan *proto.MsgBatch
	nc      proto.NodeConn
}

func (er *executorRecv) spawn() {
	var err error
	for {
		mb := <-er.recv
		if err = er.nc.ReadBatch(mb); err != nil {
			err = errors.Wrap(err, "Cluster batch read")
			mb.DoneWithError(er.cluster, er.addr, err)
		} else {
			mb.Done(er.cluster, er.addr)
		}
	}
}

func spwanPipe(addr string, cc *ClusterConfig, nb <-chan *proto.MsgBatch, nc proto.NodeConn) {
	forward := make(chan *proto.MsgBatch, len(nb))
	ed := &executorDown{
		cc:      cc,
		addr:    addr,
		input:   nb,
		forward: forward,
		nc:      nc,
		local:   make([]*proto.MsgBatch, 64),
	}
	go ed.spawn()

	er := &executorRecv{
		cluster: cc.Name,
		addr:    addr,
		recv:    forward,
		nc:      nc,
	}
	go er.spawn()
}
