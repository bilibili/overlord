package cluster

import (
	"bytes"
	errs "errors"
	"sync"
	"sync/atomic"
	"time"

	"overlord/lib/hashkit"
	"overlord/lib/log"
	libnet "overlord/lib/net"
	"overlord/proto"
	"overlord/proto/redis"

	"github.com/pkg/errors"
)

const (
	opening = int32(0)
	closed  = int32(1)

	musk = 0x3fff
)

// errors
var (
	ErrConfigServerFormat = errs.New("servers config format error")
	ErrClusterClosed      = errs.New("cluster forward already closed")
)

type cluster struct {
	name          string
	servers       []string
	conns         int32
	dto, rto, wto time.Duration
	hashTag       []byte

	slotNode atomic.Value
	action   chan struct{}

	redts map[string]*redirect
	rLock sync.Mutex

	state int32
}

// NewExecutor new Executor.
func NewExecutor(name string, servers []string, conns int32, dto, rto, wto time.Duration, hashTag []byte) proto.Executor {
	c := &cluster{}
	c.tryFetch(true)
	go c.fetchproc()
	return c
}

func (c *cluster) Execute(mba *proto.MsgBatchAllocator, msgs []*proto.Message) error {
	if state := atomic.LoadInt32(&c.state); state == closed {
		return ErrClusterClosed
	}
	for _, m := range msgs {
		if m.IsBatch() {
			for _, subm := range m.Batch() {
				addr := c.getAddr(subm.Request().Key())
				mba.AddMsg(addr, subm)
			}
		} else {
			addr := c.getAddr(m.Request().Key())
			mba.AddMsg(addr, m)
		}
	}
	for addr, mb := range mba.MsgBatchs() {
		if mb.Count() > 0 {
			// WaitGroup add one MsgBatch!!!
			mba.Add(1) // NOTE: important!!! for wait all MsgBatch done!!!
			sn := c.slotNode.Load().(*slotNode)
			sn.nodeChan[addr].push(mb)
		}
	}
	mba.Wait()
	return nil
}

func (c *cluster) Close() error {
	if !atomic.CompareAndSwapInt32(&c.state, opening, closed) {
		return nil
	}
	return nil
}

func (c *cluster) fetchproc() {
	for {
		select {
		case <-c.action:
		case <-time.After(time.Minute):
		}
		c.tryFetch(false)
	}
}

func (c *cluster) tryFetch(first bool) (changed bool) {
	for _, server := range c.servers {
		conn := libnet.DialWithTimeout(server, c.dto, c.rto, c.wto)
		f := newFetcher(conn)
		ns, err := f.fetch()
		if err != nil {
			log.Errorf("fail to fetch due to %s", err)
			continue
		}
		c.initSlotNode(ns)
		return
	}
	if first {
		panic("redis cluster all seed nodes fail to fetch")
	}
	log.Error("redis cluster all seed nodes fail to fetch")
	return
}

func (c *cluster) process(addr string) *ncChan {
	nbc := newNCChan(c.conns)
	for i := int32(0); i < c.conns; i++ {
		nc := redis.NewNodeConn(c.name, addr, c.dto, c.rto, c.wto)
		ch := nbc.get(i, nc)
		go c.processIO(c.name, addr, ch, nc)
	}
	return nbc
}

func (c *cluster) processIO(name, addr string, ch <-chan *proto.MsgBatch, nc proto.NodeConn) {
	for {
		mb, ok := <-ch
		if !ok {
			return
		}
		if err := nc.WriteBatch(mb); err != nil {
			err = errors.Wrap(err, "Cluster batch write")
			mb.DoneWithError(name, addr, err)
			continue
		}
		if err := nc.ReadBatch(mb); err != nil {
			err = errors.Wrap(err, "Cluster batch read")
			mb.DoneWithError(name, addr, err)
			continue
		}
		mb.Done(name, addr)
	}
}

func (c *cluster) getAddr(key []byte) (addr string) {
	realKey := c.trimHashTag(key)
	crc := hashkit.Crc16(realKey) & musk
	sn := c.slotNode.Load().(*slotNode)
	return sn.ns.slots[crc]
}

func (c *cluster) trimHashTag(key []byte) []byte {
	if len(c.hashTag) != 2 {
		return key
	}
	bidx := bytes.IndexByte(key, c.hashTag[0])
	if bidx == -1 {
		return key
	}
	eidx := bytes.IndexByte(key[bidx+1:], c.hashTag[1])
	if eidx == -1 {
		return key
	}
	return key[bidx+1 : bidx+1+eidx]
}

func (c *cluster) initSlotNode(ns *nodeSlots) {
	osn := c.slotNode.Load().(*slotNode) // old slotNode
	onc := map[string]*ncChan{}          // nold nodeConn
	if osn != nil {
		for addr, bc := range osn.nodeChan {
			onc[addr] = bc // COPY
		}
	}
	sn := &slotNode{}
	sn.nodeChan = make(map[string]*ncChan)
	for _, addr := range ns.getMasters() {
		bc, ok := onc[addr]
		if !ok {
			bc = c.process(addr)
		} else {
			delete(onc, addr)
		}
		sn.nodeChan[addr] = bc
	}
	c.slotNode.Store(sn)
	for _, bc := range onc {
		bc.close()
	}
}

func (c *cluster) getRedirectNodeConn(addr string) (r *redirect) {
	c.rLock.Lock()
	r, ok := c.redts[addr]
	if ok {
		c.rLock.Unlock()
		return
	}
	rnc := redis.NewNodeConn(c.name, addr, c.dto, c.rto, c.wto).(*redis.NodeConn)
	c.redts[addr] = &redirect{nc: rnc}
	c.rLock.Unlock()
	return
}

func (c *cluster) closeRedirectNodeConn(addr string, isMove bool) {
	if !isMove {
		return
	}
	c.rLock.Lock()
	r, ok := c.redts[addr]
	if ok {
		r.nc.Close()
		delete(c.redts, addr)
	}
	c.rLock.Unlock()
	select {
	case c.action <- struct{}{}:
	default:
	}
	return
}

type redirect struct {
	nc   *redis.NodeConn
	lock sync.Mutex
}

type slotNode struct {
	ns       *nodeSlots
	nodeChan map[string]*ncChan
}

type ncChan struct {
	idx   int32
	cnt   int32
	chs   []chan *proto.MsgBatch
	ncs   []proto.NodeConn
	state int32
}

func newNCChan(n int32) *ncChan {
	chs := make([]chan *proto.MsgBatch, n)
	ncs := make([]proto.NodeConn, n)
	for i := int32(0); i < n; i++ {
		chs[i] = make(chan *proto.MsgBatch, 1024)
	}
	return &ncChan{cnt: n, chs: chs, ncs: ncs}
}

func (c *ncChan) push(m *proto.MsgBatch) {
	if state := atomic.LoadInt32(&c.state); state == closed {
		return
	}
	i := atomic.AddInt32(&c.idx, 1)
	c.chs[i%c.cnt] <- m
}

func (c *ncChan) get(i int32, nc proto.NodeConn) chan *proto.MsgBatch {
	c.ncs[i] = nc
	return c.chs[i%c.cnt]
}

func (c *ncChan) close() {
	if !atomic.CompareAndSwapInt32(&c.state, opening, closed) {
		return
	}
	for i := 0; i < len(c.chs); i++ {
		close(c.chs[i])
		c.ncs[i].Close()
	}
}
