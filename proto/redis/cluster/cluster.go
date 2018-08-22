package cluster

import (
	"bytes"
	errs "errors"
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
	clusterStateOpening = int32(0)
	clusterStateClosed  = int32(1)

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

	state int32
}

// NewForward new forward.
func NewForward(name string, servers []string, conns int32, dto, rto, wto time.Duration, hashTag []byte) proto.Forwarder {
	c := &cluster{}
	c.tryFetch()
	go c.fetchproc()
	return c
}

func (c *cluster) Forward(mba *proto.MsgBatchAllocator, msgs []*proto.Message) error {
	if closed := atomic.LoadInt32(&c.state); closed == clusterStateClosed {
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
	if !atomic.CompareAndSwapInt32(&c.state, clusterStateOpening, clusterStateClosed) {
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
		c.tryFetch()
	}
}

func (c *cluster) tryFetch() (changed bool) {
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
	log.Error("redis cluster all seed nodes fail to fetch")
	return
}

func (c *cluster) process(addr string) *batchChan {
	nbc := newBatchChan(c.conns)
	for i := int32(0); i < c.conns; i++ {
		ch := nbc.get(i)
		nc := redis.NewNodeConn(c.name, addr, c.dto, c.rto, c.wto)
		go c.processIO(c.name, addr, ch, nc)
	}
	return nbc
}

func (c *cluster) processIO(name, addr string, ch <-chan *proto.MsgBatch, nc proto.NodeConn) {
	for {
		mb := <-ch
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
	sn := &slotNode{}
	sn.nodeChan = make(map[string]*batchChan)
	for _, addr := range ns.getMasters() {
		bc := c.process(addr)
		sn.nodeChan[addr] = bc
	}
	c.slotNode.Store(sn)
}

type slotNode struct {
	ns       *nodeSlots
	nodeChan map[string]*batchChan
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
