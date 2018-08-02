package proxy

import (
	"bytes"
	"overlord/lib/hashkit"
	"overlord/lib/log"
	"overlord/proto"
	"sync"

	"github.com/pkg/errors"
)

// errors
var (
	ErrProxyFail           = errors.New("proxy fail")
	ErrCacheTypeNotSupport = errors.New("this cache type is not supported")
)

// defaultExecutor implement the default hashring router and msgbatch.
type defaultExecutor struct {
	router *router

	alias  bool
	locker sync.RWMutex
	// recording alias to real node
	aliasMap map[string]string
	nodeMap  map[string]*proto.BatchChan
}

// newDefaultExecutor must combine with Start
// e.g: newDefaultExecutor().Start(cc)
func newDefaultExecutor() *defaultExecutor {
	return &defaultExecutor{}
}

var deSupportedCacheTypes = map[string]struct{}{
	proto.CacheTypeMemcache:       struct{}{},
	proto.CacheTypeMemcacheBinary: struct{}{},
	proto.CacheTypeRedis:          struct{}{},
}

func (de *defaultExecutor) Start(cc *ClusterConfig) (proto.Executor, error) {
	if _, ok := deSupportedCacheTypes[cc.CacheType]; !ok {
		return nil, ErrCacheTypeNotSupport
	}

	de.locker.Lock()
	defer de.locker.Unlock()

	addrs, ws, ans, alias, err := parseServers(cc.Servers)
	if err != nil {
		return nil, err
	}

	r := newRouter(cc.HashMethod, cc.HashMethod, []byte(cc.HashTag))
	if alias {
		r.Init(ans, ws)
	} else {
		r.Init(addrs, ws)
	}

	de.router = r

	// make alias
	if alias {
		de.aliasMap = make(map[string]string)
		for idx, aname := range ans {
			de.aliasMap[aname] = addrs[idx]
		}
	}

	// start nbc
	de.nodeMap = make(map[string]*proto.BatchChan)
	for _, node := range addrs {
		de.nodeMap[node] = de.process(cc, node)
	}
	return de, nil
}

func (de *defaultExecutor) getNode(key []byte) (string, bool) {
	backend, ok := de.router.GetNode(key)
	if !ok {
		log.Errorf("msg dispatch to MsgBatch fail")
		return "", false
	}

	node, ok := de.getAddr(backend)
	if !ok {
		log.Errorf("msg find alias map fail")
		return "", false
	}
	return node, ok

}

// Execute impl proto.Executor
func (de *defaultExecutor) Execute(mba *proto.MsgBatchAllocator, msgs []*proto.Message) error {
	de.locker.RLock()
	defer de.locker.RUnlock()

	for _, m := range msgs {
		if m.IsBatch() {
			for _, subm := range m.Batch() {
				addr, ok := de.getNode(subm.Request().Key())
				if !ok {
					m.DoneWithError(ErrProxyFail)
					return ErrProxyFail
				}
				mba.AddMsg(addr, subm)
			}
		} else {
			addr, ok := de.getNode(m.Request().Key())
			if !ok {
				m.DoneWithError(ErrProxyFail)
				return ErrProxyFail
			}
			mba.AddMsg(addr, m)
		}
	}

	// TODO: use quick search to make iterator fast
	for node, mb := range mba.MsgBatchs() {
		if mb.Count() > 0 {
			de.nodeMap[node].Push(mb)
		}
	}

	return nil
}

// process will start the special backend connection.
func (de *defaultExecutor) process(cc *ClusterConfig, addr string) *proto.BatchChan {
	conns := cc.NodeConnections
	nbc := proto.NewBatchChan(conns)
	for i := 0; i < int(conns); i++ {
		ch := nbc.GetCh(i)
		nc := newNodeConn(cc, addr)
		go de.processIO(cc.Name, addr, ch, nc)
	}
	return nbc
}

func (de *defaultExecutor) processIO(cluster, addr string, ch <-chan *proto.MsgBatch, nc proto.NodeConn) {
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
		mb.BatchDone(cluster, addr)
	}
}

func (de *defaultExecutor) getAddr(backend string) (string, bool) {
	if de.alias {
		node, ok := de.aliasMap[backend]
		return node, ok
	}
	return backend, true
}

type router struct {
	// thread safe ring type
	ring    *hashkit.HashRing
	hashTag []byte
}

func newRouter(dist, method string, hashTag []byte) *router {
	ring := hashkit.NewRing(dist, method)
	return &router{ring: ring, hashTag: hashTag}
}

func (r *router) Init(names []string, ws []int) {
	r.ring.Init(names, ws)
}

// GetNode will get the node/alias by given key.
func (r *router) GetNode(key []byte) (string, bool) {
	realkey := r.trimHashTag(key)
	return r.ring.GetNode(realkey)
}

func (r *router) trimHashTag(key []byte) []byte {
	if len(r.hashTag) != 2 {
		return key
	}
	bidx := bytes.IndexByte(key, r.hashTag[0])
	if bidx == -1 {
		return key
	}
	eidx := bytes.IndexByte(key[bidx+1:], r.hashTag[1])
	if eidx == -1 {
		return key
	}

	return key[bidx+1 : bidx+1+eidx]
}
