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
	fakeClusterNodes = "" +
		"0000000000000000000000000000000000000001 {ADDR} master - 0 0 1 connected 0-5460\n" +
		"0000000000000000000000000000000000000002 {ADDR} master - 0 0 2 connected 5461-10922\n" +
		"0000000000000000000000000000000000000003 {ADDR} master - 0 0 3 connected 10923-16383\n"

	fakeClusterSlots = "" +
		"*3\r\n" +
		"*3\r\n:0\r\n:5460\r\n*2\r\n${IPLEN}\r\n{IP}\r\n:{PORT}\r\n" +
		"*3\r\n:5460\r\n:10922\r\n*2\r\n${IPLEN}\r\n{IP}\r\n:{PORT}\r\n" +
		"*3\r\n:10922\r\n:16383\r\n*2\r\n${IPLEN}\r\n{IP}\r\n:{PORT}\r\n"
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

	fakeNodesBytes []byte
	fakeSlotsBytes []byte
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
	if !c.tryFetch() {
		panic("redis cluster all seed nodes fail to fetch")
	}
	c.fake(listen)
	go c.fetchproc()
	return c
}

func (c *cluster) Execute(mba *proto.MsgBatchAllocator, msgs []*proto.Message, nodem map[string]struct{}) error {
	if state := atomic.LoadInt32(&c.state); state == closed {
		return ErrClusterClosed
	}
	for _, m := range msgs {
		if m.IsBatch() {
			for _, subm := range m.Batch() {
				addr := c.getAddr(subm.Request().Key())
				mba.AddMsg(addr, subm)
				nodem[addr] = struct{}{}
			}
		} else {
			addr := c.getAddr(m.Request().Key())
			mba.AddMsg(addr, m)
			nodem[addr] = struct{}{}
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
		ncCh := &ncChan{nc: newNodeConn(c, addr), stop: make(chan struct{})}
		nbc.add(i, ncCh)
		go c.processIO(c.name, addr, nbc.ch, ncCh)
	}
	return nbc
}

func (c *cluster) processIO(name, addr string, ch chan *proto.MsgBatch, ncCh *ncChan) {
	var (
		nc  = ncCh.nc
		err error
	)
	for {
		if err != nil {
			nc = newNodeConn(c, addr)
			// TODO: there may be chaos with unexcepted closed of cluster.
			ncCh.nc = nc
			// fire try fetch action
			select {
			case c.action <- struct{}{}:
			default:
			}
		}
		mb, ok := <-ch
		if !ok {
			close(ncCh.stop) // NOTE: close stop, make sure nc closed concurrent security!!!
			return
		}
		if err = nc.WriteBatch(mb); err != nil {
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
		c.tryFetch()
	}
}

func (c *cluster) tryFetch() bool {
	// for map's access is random in golang.
	shuffleMap := make(map[string]struct{})
	for _, server := range c.servers {
		shuffleMap[server] = struct{}{}
	}
	for server := range shuffleMap {
		conn := libnet.DialWithTimeout(server, c.dto, c.rto, c.wto)
		f := newFetcher(conn)
		nSlots, err := f.fetch()
		if err != nil {
			log.Errorf("fail to fetch due to %s", err)
			continue
		}
		c.initSlotNode(nSlots)
		return true
	}

	log.Error("redis cluster all seed nodes fail to fetch")
	return false
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
	masters := nSlots.getMasters()
	for _, addr := range masters {
		bc, ok := onc[addr]
		if !ok {
			bc = c.process(addr)
		} else {
			delete(onc, addr)
		}
		sn.nodeChan[addr] = bc
	}
	c.servers = masters
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
		_ = r.nc.Close()
		r.nc = nil
		r.lock.Unlock() // FIXME(felix): when NodeConn have nc pointer in func redirectProcess
		delete(c.redts, addr)
	}
	c.rLock.Unlock()
	select {
	case c.action <- struct{}{}:
	default:
	}
}

func (c *cluster) fake(listen string) {
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
					ipStr := ipnet.IP.String()

					ipStrLen := len(ipStr)
					slotsStr := strings.Replace(fakeClusterSlots, "{IPLEN}", strconv.Itoa(ipStrLen), -1)
					slotsStr = strings.Replace(slotsStr, "{IP}", ipStr, -1)
					slotsStr = strings.Replace(slotsStr, "{PORT}", port, -1)
					c.fakeSlotsBytes = []byte(slotsStr)

					nodesStr := strings.Replace(fakeClusterNodes, "{ADDR}", net.JoinHostPort(ipStr, port), -1)
					nodesLen := len(nodesStr)
					c.fakeNodesBytes = []byte("$" + strconv.Itoa(nodesLen) + "\r\n" + nodesStr + "\r\n")
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
	ch    chan *proto.MsgBatch
	ncChs map[int32]*ncChan
	state int32
}

func newBatchChan(n int32) *batchChan {
	return &batchChan{ch: make(chan *proto.MsgBatch, n*1024), ncChs: make(map[int32]*ncChan)}
}

func (c *batchChan) push(m *proto.MsgBatch) {
	if state := atomic.LoadInt32(&c.state); state == closed {
		return
	}
	c.ch <- m
}

func (c *batchChan) add(i int32, ncCh *ncChan) {
	c.ncChs[i] = ncCh
}

func (c *batchChan) close() {
	if !atomic.CompareAndSwapInt32(&c.state, opening, closed) {
		return
	}
	close(c.ch)
	for _, ncCh := range c.ncChs {
		<-ncCh.stop // NOTE: wait stop closed, make sure nc closed concurrent security!!!
		_ = ncCh.nc.Close()
	}
}

type ncChan struct {
	nc   proto.NodeConn
	stop chan struct{}
}
