package cluster

import (
	"bytes"
	errs "errors"
	"net"
	"strconv"
	"strings"
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
	ErrClusterClosed = errs.New("cluster executor already closed")
)

const (
	flashyClusterNodes = "" +
		"b1798ba2171a4bd765846ddb5d5bdc9f3ca6fdf3 {} master - 0 0 1 connected 0-5460\r\n" +
		"dff2f7b0fbda82c72d426eeb9616d9d6455bb4ff {} master - 0 0 2 connected 5461-10922\r\n" +
		"828c400ea2b55c43e5af67af94bec4943b7b3d93 {} master - 0 0 3 connected 10923-16383\r\n"
)

var (
	flashyClusterNodesResp []byte
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

	once sync.Once
}

// NewExecutor new Executor.
func NewExecutor(name, listen string, servers []string, conns int32, dto, rto, wto time.Duration, hashTag []byte) proto.Executor {
	c := &cluster{
		name:    name,
		servers: servers,
		conns:   conns,
		dto:     dto,
		rto:     rto,
		wto:     wto,
		hashTag: hashTag,
		action:  make(chan struct{}),
		redts:   make(map[string]*redirect),
	}
	c.tryFetch(true)
	c.flashy(listen)
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

func (c *cluster) process(addr string) *batchChan {
	nbc := newBatchChan(c.conns)
	for i := int32(0); i < c.conns; i++ {
		nc := newNodeConn(c, addr)
		ncCh := nbc.get(i, nc)
		go c.processIO(c.name, addr, ncCh)
	}
	return nbc
}

func (c *cluster) processIO(name, addr string, ncCh *ncChan) {
	var (
		ch = ncCh.ch
		nc = ncCh.nc
	)
	for {
		mb, ok := <-ch
		if !ok {
			close(ncCh.stop) // NOTE: close stop, make sure nc closed concurrent security!!!
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
	return sn.nSlots.slots[crc]
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

func (c *cluster) fetchproc() {
	for {
		select {
		case <-c.action:
		case <-time.After(1 * time.Minute):
		}
		c.tryFetch(false)
	}
}

func (c *cluster) tryFetch(first bool) (changed bool) {
	for _, server := range c.servers {
		conn := libnet.DialWithTimeout(server, c.dto, c.rto, c.wto)
		f := newFetcher(conn)
		nSlots, err := f.fetch()
		if err != nil {
			log.Errorf("fail to fetch due to %s", err)
			continue
		}
		c.initSlotNode(nSlots)
		return
	}
	if first {
		panic("redis cluster all seed nodes fail to fetch")
	}
	log.Error("redis cluster all seed nodes fail to fetch")
	return
}

func (c *cluster) initSlotNode(nSlots *nodeSlots) {
	osn, ok := c.slotNode.Load().(*slotNode) // old slotNode
	onc := map[string]*batchChan{}           // old nodeConn
	if ok && osn != nil {
		for addr, bc := range osn.nodeChan {
			onc[addr] = bc // COPY
		}
	}
	sn := &slotNode{nSlots: nSlots}
	sn.nodeChan = make(map[string]*batchChan)
	for _, addr := range nSlots.getMasters() {
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
	r = &redirect{nc: rnc}
	c.redts[addr] = r
	c.rLock.Unlock()
	return
}

func (c *cluster) closeRedirectNodeConn(addr string, isAsk bool) {
	if isAsk {
		return
	}
	c.rLock.Lock()
	r, ok := c.redts[addr]
	if ok {
		r.lock.Lock()
		r.nc.Close()
		r.nc = nil
		r.lock.Unlock() // FIXME(felix): when NodeConn have nc pointer in func redirectProcess
		delete(c.redts, addr)
	}
	c.rLock.Unlock()
	select {
	case c.action <- struct{}{}:
	default:
	}
	return
}

func (c *cluster) flashy(listen string) {
	c.once.Do(func() {
		_, port, err := net.SplitHostPort(listen)
		if err != nil {
			panic(err)
		}
		inters, err := net.Interfaces()
		if err != nil {
			panic(err)
		}
		for _, inter := range inters {
			if strings.HasPrefix(inter.Name, "lo") {
				continue
			}
			addrs, err := inter.Addrs()
			if err != nil {
				continue
			}
			for _, addr := range addrs {
				if ipnet, ok := addr.(*net.IPNet); ok && !ipnet.IP.IsLoopback() && ipnet.IP.To4() != nil {
					ipPort := net.JoinHostPort(ipnet.IP.String(), port)
					respStr := strings.Replace(flashyClusterNodes, "{}", ipPort, -1)
					l := len(respStr)
					flashyClusterNodesResp = []byte("$" + strconv.Itoa(l) + "\r\n" + respStr + "\r\n")
					return
				}
			}
		}
	})
}

type redirect struct {
	nc   *redis.NodeConn
	lock sync.Mutex
}

type slotNode struct {
	nSlots   *nodeSlots
	nodeChan map[string]*batchChan
}

type batchChan struct {
	idx   int32
	cnt   int32
	ncChs []*ncChan
	state int32
}

type ncChan struct {
	ch   chan *proto.MsgBatch
	nc   proto.NodeConn
	stop chan struct{}
}

func newBatchChan(n int32) *batchChan {
	ncChs := make([]*ncChan, n)
	for i := int32(0); i < n; i++ {
		ncChs[i] = &ncChan{
			ch:   make(chan *proto.MsgBatch, 1024),
			stop: make(chan struct{}),
		}
	}
	return &batchChan{cnt: n, ncChs: ncChs}
}

func (c *batchChan) push(m *proto.MsgBatch) {
	if state := atomic.LoadInt32(&c.state); state == closed {
		return
	}
	i := atomic.AddInt32(&c.idx, 1)
	c.ncChs[i%c.cnt].ch <- m
}

func (c *batchChan) get(i int32, nc proto.NodeConn) *ncChan {
	c.ncChs[i].nc = nc
	return c.ncChs[i%c.cnt]
}

func (c *batchChan) close() {
	if !atomic.CompareAndSwapInt32(&c.state, opening, closed) {
		return
	}
	for i := 0; i < len(c.ncChs); i++ {
		close(c.ncChs[i].ch)
		<-c.ncChs[i].stop // NOTE: wait stop closed, make sure nc closed concurrent security!!!
		c.ncChs[i].nc.Close()
	}
}
