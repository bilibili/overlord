package cluster

import (
	"bytes"
	"errors"
	"overlord/proto"
	"overlord/proto/redis"
	"strings"
	"sync"
	"time"
)

const musk = 0x3fff

// errors
var (
	ErrProxyDispatchFail = errors.New("fail to dispatch")
)

// RedisClusterConfig was used as config to avoid cycle import
type RedisClusterConfig struct {
	// Cluster Name
	Cluster string
	Servers []string

	// ReadTimeout is read timeout config of node connection.
	ReadTimeout time.Duration
	// WriteTimeout is write timeout config of node connection.
	WriteTimeout time.Duration
	// DialTimeout is dial timeout config of node connection.
	DialTimeout time.Duration
}

type redisClusterExecutor struct {
	rcc  *RedisClusterConfig
	smap *slotsMap

	nodeMap map[string]*proto.BatchChan
	locker  sync.RWMutex
}

// StartExecutor will start new redis executor
func StartExecutor(rcc *RedisClusterConfig) (proto.Executor, error) {
	return &redisClusterExecutor{}, nil
}

func (re *redisClusterExecutor) start(rcc *RedisClusterConfig) error {
	// servers := rcc.Servers
	return nil
}

// Execute impl proto.Executor
func (re *redisClusterExecutor) Execute(mba *proto.MsgBatchAllocator, msgs []*proto.Message) (err error) {
	re.locker.RLock()
	defer re.locker.RUnlock()

	for _, m := range msgs {
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
			re.nodeMap[node].Push(mb)
		}
	}

	return nil
}

func (re *redisClusterExecutor) getMaster(key []byte) string {
	return re.smap.getMaster(key)
}

type slotsMap struct {
	slots   []string
	crc     uint16
	hashTag []byte
}

func newSlotsMap(slots []string, hashTag []byte) *slotsMap {
	s := &slotsMap{slots: slots, hashTag: hashTag}
	sb := new(strings.Builder)
	for _, addr := range slots {
		_, _ = sb.WriteString(addr)
	}
	s.crc = Crc16([]byte(sb.String()))
	return s
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
