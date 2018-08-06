package proxy

import (
	"context"
	errs "errors"
	"net"
	"strings"
	"sync/atomic"
	"time"

	"overlord/lib/conv"
	"overlord/lib/log"
	"overlord/proto"
	"overlord/proto/memcache"
	mcbin "overlord/proto/memcache/binary"
	"overlord/proto/redis"
	rc "overlord/proto/redis/cluster"
)

const (
	clusterStateOpening = uint32(0)
	clusterStateClosed  = uint32(1)
)

// cluster errors
var (
	ErrClusterServerFormat = errs.New("cluster servers format error")
	ErrClusterHashNoNode   = errs.New("cluster hash no hit node")
)

type pinger struct {
	ping   proto.NodeConn
	cc     *ClusterConfig
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

	executor proto.Executor

	state uint32
}

// NewCluster new a cluster by cluster config.
func NewCluster(ctx context.Context, cc *ClusterConfig) (c *Cluster) {
	c = &Cluster{cc: cc, state: clusterStateOpening}
	if c.cc.CacheType != proto.CacheTypeRedisCluster {
		exec, err := newDefaultExecutor().Start(c.cc)
		if err != nil {
			log.Errorf("fail to start defaultExecutor by %s", err)
			return
		}
		c.executor = exec
	} else {
		log.Info("start to init executor with redis cluster executor")
		exec, err := rc.StartExecutor(c.cc.AsRedisClusterConfig())
		if err != nil {
			log.Errorf("fail to init redis cluster executor %s", err)
			return
		}
		c.executor = exec
	}
	c.ctx, c.cancel = context.WithCancel(ctx)
	// if c.cc.PingAutoEject {
	// go c.startPinger(c.cc, addrs, ws)
	// }
	return
}

// Execute impl proto.Executor and forword it into inner executor
func (c *Cluster) Execute(mba *proto.MsgBatchAllocator, msgs []*proto.Message) error {
	return c.executor.Execute(mba, msgs)
}

// func (c *Cluster) startPinger(cc *ClusterConfig, addrs []string, ws []int) {
// 	for idx, addr := range addrs {
// 		w := ws[idx]
// 		nc := newNodeConn(cc, addr)
// 		p := &pinger{ping: nc, node: addr, weight: w}
// 		go c.processPing(p)
// 	}
// }

// func (c *Cluster) processPing(p *pinger) {
// 	del := false
// 	for {
// 		select {
// 		case <-c.ctx.Done():
// 			return
// 		default:
// 		}
// 		if err := p.ping.Ping(); err != nil {
// 			p.failure++
// 			p.retries = 0
// 			log.Warnf("node ping fail:%d times with err:%v", p.failure, err)
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

// // hash returns node by hash hit.
// func (c *Cluster) hash(key []byte) (node string, ok bool) {
// 	var realKey []byte
// 	if len(c.hashTag) == 2 {
// 		if b := bytes.IndexByte(key, c.hashTag[0]); b >= 0 {
// 			if e := bytes.IndexByte(key[b+1:], c.hashTag[1]); e >= 0 {
// 				realKey = key[b+1 : b+1+e]
// 			}
// 		}
// 	}
// 	if len(realKey) == 0 {
// 		realKey = key
// 	}
// 	node, ok = c.ring.GetNode(realKey)
// 	return
// }

// Close closes resources.
func (c *Cluster) Close() error {
	if atomic.CompareAndSwapUint32(&c.state, clusterStateOpening, clusterStateClosed) {
		c.cancel()
	}
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
