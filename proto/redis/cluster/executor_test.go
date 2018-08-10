package cluster

import (
	"fmt"
	"overlord/proto/redis"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestSlotsMapNewSlotsMap(t *testing.T) {
	s := newSlotsMap([]string{"abc", "def", "baka"}, []byte("{}"))
	assert.Equal(t, uint16(0x16f7), s.crc)
}

func TestSlotsMapGetMaster(t *testing.T) {
	slots := make([]string, 16384)
	for idx := range slots {
		slots[idx] = fmt.Sprintf("node-%d", idx)
	}
	s := newSlotsMap(slots, []byte(""))
	node := s.getMaster([]byte("a"))
	assert.Equal(t, "node-15495", node)
}

func TestSlotsMapTrimHash(t *testing.T) {
	slots := make([]string, 16384)
	for idx := range slots {
		slots[idx] = fmt.Sprintf("node-%d", idx)
	}
	// key 'a' slot is 15495
	s := newSlotsMap(slots, []byte("{}"))
	node := s.getMaster([]byte("a"))
	assert.Equal(t, "node-15495", node)

	// key 'a' slot is 15495
	node = s.getMaster([]byte("{a}"))
	assert.Equal(t, "node-15495", node)
	// key '{a' slot is 15495
	node = s.getMaster([]byte("{a"))
	assert.Equal(t, "node-10276", node)
}

var _testRCC = &RedisClusterConfig{
	Cluster: "test-redis-cluster",
	// unreachable addr
	Servers:         []string{"127.0.0.1:7777"},
	HashTag:         []byte("{}"),
	NodeConnections: int32(1),
	ReadTimeout:     time.Second,
	WriteTimeout:    time.Second,
	DialTimeout:     time.Second,
	FetchInterval:   time.Minute,
}

func TestRedisClusterExecutorTryFetchFail(t *testing.T) {
	rc := &redisClusterExecutor{rcc: _testRCC}
	err := rc.tryFetch()
	assert.Error(t, err)
}

var clusterNodesData = `3f76d4dca41307bea25e8f69a3545594479dc7a9 172.17.0.2:7004@17004 slave ec433a34a97e09fc9c22dd4b4a301e2bca6602e0 0 1528252310916 5 connected
f17c3861b919c58b06584a0778c4f60913cf213c 172.17.0.2:7005@17005 slave 91240f5f82621d91d55b02d3bc1dcd1852dc42dd 0 1528252309896 6 connected
91240f5f82621d91d55b02d3bc1dcd1852dc42dd 172.17.0.2:7002@17002 master - 0 1528252310000 3 connected 10923-16383
ec433a34a97e09fc9c22dd4b4a301e2bca6602e0 172.17.0.2:7001@17001 master - 0 1528252310606 2 connected 5461-10922
a063bbdc2c4abdc60e09fdf1934dc8c8fb2d69df 172.17.0.2:7003@17003 slave a8f85c7b9a2e2cd24dda7a60f34fd889b61c9c00 0 1528252310506 4 connected
a8f85c7b9a2e2cd24dda7a60f34fd889b61c9c00 172.17.0.2:7000@17000 myself,master - 0 1528252310000 1 connected 0-5460`

func TestRedisCLusterExecutorDoFetchOk(t *testing.T) {
	sb := new(strings.Builder)
	sb.WriteString("$")
	sb.WriteString(fmt.Sprintf("%d\r\n", len(clusterNodesData)))
	sb.WriteString(clusterNodesData)
	sb.WriteString("\r\n")
	rc := &redisClusterExecutor{rcc: _testRCC, smap: &slotsMap{crc: crc16NoneValue}}
	err := rc.doFetch(redis.NewFetcher(_createConn([]byte(sb.String()))))
	assert.NoError(t, err)
}
