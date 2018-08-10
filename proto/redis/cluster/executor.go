package cluster

import (
	"bytes"
	"overlord/lib/log"
	libnet "overlord/lib/net"
	"overlord/proto"
	"overlord/proto/redis"
	"strings"
	"sync"
	"time"

	"math"

	"github.com/pkg/errors"
)

const (
	musk           = 0x3fff
	crc16NoneValue = math.MaxUint16
)

// errors
var (
	ErrProxyDispatchFail = errors.New("fail to dispatch")
)

// RedisClusterConfig was used as config to avoid cycle import
type RedisClusterConfig struct {
	// Cluster Name
	Cluster string
	Servers []string
	HashTag []byte

	NodeConnections int32

	// ReadTimeout is read timeout config of node connection.
	ReadTimeout time.Duration
	// WriteTimeout is write timeout config of node connection.
	WriteTimeout time.Duration
	// DialTimeout is dial timeout config of node connection.
	DialTimeout time.Duration

	// FetchInterval is the duration for each fetch of overlord, default is 10 minutes.
	FetchInterval time.Duration
}

type redisClusterExecutor struct {
	rcc  *RedisClusterConfig
	smap *slotsMap

	nodeMap map[string]*proto.BatchChan
	locker  sync.RWMutex
}

// StartExecutor will start new redis executor
func StartExecutor(rcc *RedisClusterConfig) (proto.Executor, error) {
	re := &redisClusterExecutor{rcc: rcc}
	err := re.start()
	return re, err
}

func (re *redisClusterExecutor) doFetch(f *redis.Fetcher) (err error) {
	var (
		data []byte
		ns   *redis.NodeSlots
	)

	data, err = f.Fetch()
	if err != nil {
		log.Errorf("fail to fetch due to %s", err)
		return
	}

	ns, err = redis.ParseSlots(data)
	if err != nil {
		log.Errorf("fail to parse Cluster Nodes data due %s", err)
		return
	}

	sm := newSlotsMap(ns.GetSlots(), re.rcc.HashTag)
	re.locker.Lock()
	if sm.crc != re.smap.crc {
		log.Info("update slotsMap due to crc flag is not the the same.")
		re.smap = sm
	}
	re.locker.Unlock()
	return
}

func (re *redisClusterExecutor) tryFetch() (err error) {
	for _, server := range re.rcc.Servers {
		conn := libnet.DialWithTimeout(server, re.rcc.DialTimeout, re.rcc.ReadTimeout, re.rcc.WriteTimeout)
		f := redis.NewFetcher(conn)
		err = re.doFetch(f)
		if err != nil {
			continue
		}
	}
	return
}

func (re *redisClusterExecutor) processFetch() {
	var err error
	for {
		err = re.tryFetch()
		if err != nil {
			log.Errorf("fail to fetch for cluster %s with seeds %s", re.rcc.Cluster, re.rcc.Servers)
		}
		time.Sleep(re.rcc.FetchInterval)
	}
}

func (re *redisClusterExecutor) start() error {
	re.locker.Lock()
	defer re.locker.Unlock()

	var (
		ns   *redis.NodeSlots
		sm   *slotsMap
		data []byte
		err  error
	)

	for _, server := range re.rcc.Servers {
		conn := libnet.DialWithTimeout(server, re.rcc.DialTimeout, re.rcc.ReadTimeout, re.rcc.WriteTimeout)
		f := redis.NewFetcher(conn)
		data, err = f.Fetch()
		if err != nil {
			log.Errorf("fail to start new processIO goroutione of %s", err)
			continue
		}
		ns, err = redis.ParseSlots(data)
		if err != nil {
			log.Errorf("fail to parse Cluster Nodes data due %s", err)
			continue
		}

		sm = newSlotsMap(ns.GetSlots(), re.rcc.HashTag)
		break
	}
	if sm == nil {
		panic("all seed nodes fail to connected, shutdown")
	}
	re.smap = sm

	nmap := make(map[string]*proto.BatchChan)
	for _, node := range ns.GetMasters() {
		nbc := re.startProcess(node)
		nmap[node] = nbc
	}
	re.nodeMap = nmap
	// Start Fetch process
	go re.processFetch()
	return nil
}

func (re *redisClusterExecutor) startProcess(addr string) *proto.BatchChan {
	nbc := proto.NewBatchChan(re.rcc.NodeConnections)
	count := int(re.rcc.NodeConnections)
	for i := 0; i < count; i++ {
		ch := nbc.GetCh(i)
		nc := newRedisNodeConn(re.rcc, addr)
		go re.processIO(re.rcc.Cluster, addr, ch, nc)
	}
	return nbc
}

func (re *redisClusterExecutor) processIO(cluster, addr string, ch <-chan *proto.MsgBatch, nc proto.NodeConn) {
	for {
		mb := <-ch
		if err := nc.WriteBatch(mb); err != nil {
			err = errors.Wrap(err, "Cluster batch write")
			mb.BatchDoneWithError(cluster, addr, err)
			continue
		}
		err := nc.ReadBatch(mb)
		if err == redis.ErrRedirect {
			re.redirect(mb)
			continue
		} else if err != nil {
			err = errors.Wrap(err, "Cluster batch read")
			mb.BatchDoneWithError(cluster, addr, err)
			continue
		}

		mb.BatchDone(cluster, addr)
	}
}

func (re *redisClusterExecutor) redirect(mb *proto.MsgBatch) {
	redirectMap := make(map[string]*proto.MsgBatch)
	for _, m := range mb.Msgs() {
		req, _ := m.Request().(*redis.Request)
		if req.Redirect != nil {
			if !req.Redirect.IsAsk {
				// is moved
				re.locker.Lock()
				re.smap.slots[req.Redirect.Slot] = req.Redirect.Addr
				re.smap.crc = crc16NoneValue
				re.locker.Unlock()
			}

			if smb, ok := redirectMap[req.Redirect.Addr]; ok {
				smb.AddMsg(m)
			} else {
				smb := mb.Fork()
				smb.AddMsg(m)
				redirectMap[req.Redirect.Addr] = smb
			}
		}
	}

	for node, mb := range redirectMap {
		re.deliver(node, mb)
	}
}

// Execute impl proto.Executor
func (re *redisClusterExecutor) Execute(mba *proto.MsgBatchAllocator, msgs []*proto.Message) (err error) {
	for _, m := range msgs {
		if m.Err() != nil {
			// skip to dispatch with error commands
			mba.Done()
			continue
		}

		if m.IsBatch() {
			for _, subm := range m.Batch() {
				node := re.getMaster(subm.Request().Key())
				mba.AddMsg(node, subm)
			}
		} else {
			node := re.getMaster(m.Request().Key())
			mba.AddMsg(node, m)
		}
	}

	for node, mb := range mba.MsgBatchs() {
		if mb.Count() > 0 {
			re.deliver(node, mb)
		}
	}

	return nil
}

func (re *redisClusterExecutor) deliver(node string, mb *proto.MsgBatch) {
	re.locker.Lock()
	if ch, ok := re.nodeMap[node]; ok {
		ch.Push(mb)
	} else {
		ch := re.startProcess(node)
		re.nodeMap[node] = ch
		ch.Push(mb)
	}
	re.locker.Unlock()
}

func (re *redisClusterExecutor) getMaster(key []byte) (master string) {
	re.locker.RLock()
	master = re.smap.getMaster(key)
	re.locker.RUnlock()
	return
}

type slotsMap struct {
	slots   []string
	crc     uint16
	hashTag []byte
}

func newSlotsMap(slots []string, hashTag []byte) *slotsMap {
	s := &slotsMap{slots: slots, hashTag: hashTag, crc: crc16NoneValue}
	s.calcCrc()
	return s
}

func (sm *slotsMap) calcCrc() {
	sb := new(strings.Builder)
	for _, addr := range sm.slots {
		_, _ = sb.WriteString(addr)
	}
	sm.crc = Crc16([]byte(sb.String())) & musk
}

func (sm *slotsMap) getMaster(key []byte) string {
	realKey := sm.trimHashTag(key)
	crc := Crc16(realKey) & musk
	return sm.slots[crc]
}

func (sm *slotsMap) trimHashTag(key []byte) []byte {
	if len(sm.hashTag) != 2 {
		return key
	}
	bidx := bytes.IndexByte(key, sm.hashTag[0])
	if bidx == -1 {
		return key
	}
	eidx := bytes.IndexByte(key[bidx+1:], sm.hashTag[1])
	if eidx == -1 {
		return key
	}

	return key[bidx+1 : bidx+1+eidx]
}

func newRedisNodeConn(rcc *RedisClusterConfig, addr string) proto.NodeConn {
	nc := redis.NewNodeConn(rcc.Cluster, addr, rcc.DialTimeout, rcc.ReadTimeout, rcc.WriteTimeout)
	return nc
}
