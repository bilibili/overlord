package proxy

import (
	"github.com/bradfitz/gomemcache/memcache"
	"github.com/gomodule/redigo/redis"
	"github.com/stretchr/testify/assert"

	"overlord/pkg/types"
	"overlord/proxy"

	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"net"
	"testing"
	"time"
)

// GLOBAL configs
var ccs = []*proxy.ClusterConfig{
	&proxy.ClusterConfig{
		Name:             "eject-cluster",
		HashMethod:       "fnv1a_64",
		HashDistribution: "ketama",
		HashTag:          "",
		CacheType:        types.CacheType("memcache"),
		ListenProto:      "tcp",
		ListenAddr:       "127.0.0.1:22211",
		RedisAuth:        "",
		DialTimeout:      100,
		ReadTimeout:      100,
		NodeConnections:  10,
		NodePipeCount:    32,
		WriteTimeout:     1000,
		PingFailLimit:    3,
		PingAutoEject:    true,
		Servers: []string{
			"127.0.0.1:11211:10 mc1",
			// "127.0.0.1:11213:10",
		},
	},
	&proxy.ClusterConfig{
		Name:             "mc-cluster",
		HashMethod:       "fnv1a_64",
		HashDistribution: "ketama",
		HashTag:          "",
		CacheType:        types.CacheType("memcache"),
		ListenProto:      "tcp",
		ListenAddr:       "127.0.0.1:21211",
		RedisAuth:        "",
		DialTimeout:      100,
		ReadTimeout:      100,
		NodeConnections:  10,
		NodePipeCount:    32,
		WriteTimeout:     1000,
		PingFailLimit:    3,
		PingAutoEject:    false,
		Servers: []string{
			"127.0.0.1:11211:10 mc1",
			"127.0.0.1:11211:10 mc2",
			// "127.0.0.1:11213:10",
		},
	},
	&proxy.ClusterConfig{
		Name:             "mcbin-cluster",
		HashMethod:       "fnv1a_64",
		HashDistribution: "ketama",
		HashTag:          "",
		CacheType:        types.CacheType("memcache_binary"),
		ListenProto:      "tcp",
		ListenAddr:       "127.0.0.1:21212",
		RedisAuth:        "",
		DialTimeout:      100,
		ReadTimeout:      100,
		NodeConnections:  10,
		NodePipeCount:    32,
		WriteTimeout:     1000,
		PingFailLimit:    3,
		PingAutoEject:    false,
		Servers: []string{
			"127.0.0.1:11211:10",
			// "127.0.0.1:11212:10",
			// "127.0.0.1:11213:10",
		},
	},
	&proxy.ClusterConfig{
		Name:             "redis",
		HashMethod:       "fnv1a_64",
		HashDistribution: "ketama",
		HashTag:          "",
		CacheType:        types.CacheType("redis"),
		ListenProto:      "tcp",
		ListenAddr:       "127.0.0.1:26379",
		RedisAuth:        "",
		DialTimeout:      100,
		ReadTimeout:      100,
		NodeConnections:  10,
		NodePipeCount:    32,
		WriteTimeout:     1000,
		PingFailLimit:    3,
		PingAutoEject:    false,
		Servers: []string{
			"127.0.0.1:6379:10",
		},
	},
	&proxy.ClusterConfig{
		Name:             "redis-cluster",
		HashMethod:       "fnv1a_64",
		HashDistribution: "ketama",
		HashTag:          "",
		CacheType:        types.CacheType("redis_cluster"),
		ListenProto:      "tcp",
		ListenAddr:       "127.0.0.1:27000",
		RedisAuth:        "",
		DialTimeout:      100,
		ReadTimeout:      100,
		NodeConnections:  10,
		NodePipeCount:    32,
		WriteTimeout:     1000,
		PingFailLimit:    3,
		PingAutoEject:    false,
		Servers: []string{
			"127.0.0.1:7000",
			"127.0.0.1:7001",
		},
	},
	&proxy.ClusterConfig{
		Name:             "no avaliable node ",
		HashMethod:       "fnv1a_64",
		HashDistribution: "ketama",
		HashTag:          "",
		CacheType:        types.CacheType("redis"),
		ListenProto:      "tcp",
		ListenAddr:       "127.0.0.1:26380",
		RedisAuth:        "",
		DialTimeout:      100,
		ReadTimeout:      100,
		NodeConnections:  10,
		NodePipeCount:    32,
		WriteTimeout:     1000,
		PingFailLimit:    3,
		PingAutoEject:    false,
		Servers:          []string{
			//"127.0.0.1:6379:10",
			// "127.0.0.1:11212:10",
			// "127.0.0.1:11213:10",
		},
	},
	&proxy.ClusterConfig{
		Name:             "reconn_test",
		HashMethod:       "fnv1a_64",
		HashDistribution: "ketama",
		HashTag:          "",
		CacheType:        types.CacheType("memcache"),
		ListenProto:      "tcp",
		ListenAddr:       "127.0.0.1:21221",
		RedisAuth:        "",
		DialTimeout:      100,
		ReadTimeout:      100,
		NodeConnections:  1,
		NodePipeCount:    32,
		WriteTimeout:     1000,
		PingFailLimit:    3,
		PingAutoEject:    false,
		Servers: []string{
			"127.0.0.1:21220:1",
		},
	},
}

var (
	redisCommands = [][]interface{}{
		{"mset", "a", 1, "b", 20},
		{"get", "a"},
		{"del", "a"},
		{"mget", "a", "b"},
		{"del", "b"},
	}

	mcBinCommands = [][]byte{
		[]byte{
			0x80,       // magic
			0x01,       // cmd
			0x00, 0x03, // key len
			0x08,       // extra len
			0x00,       // data type
			0x00, 0x00, // vbucket
			0x00, 0x00, 0x00, 0x10, // body len
			0x00, 0x00, 0x00, 0x00, // opaque
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // cas
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // extra: flags, expiration
			0x41, 0x42, 0x43, // key: ABC
			0x41, 0x42, 0x43, 0x44, 0x45, // value: ABCDE
		},
		[]byte{
			0x80,       // magic
			0x0c,       // cmd
			0x00, 0x03, // key len
			0x00,       // extra len
			0x00,       // data type
			0x00, 0x00, // vbucket
			0x00, 0x00, 0x00, 0x03, // body len
			0x00, 0x00, 0x00, 0x00, // opaque
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // cas
			0x41, 0x42, 0x43, // key: ABC
		},
	}
)

// =============================== prapare environments ==============

func setupTest(t *testing.T) bool {
	bootCaches(t)
	mockProxy(t)
	return true
}

func tearDownTest(t *testing.T) bool {
	return true
}

func mockProxy(t *testing.T) {
	p, err := proxy.New(proxy.DefaultConfig())
	if err != nil {
		panic(err)
	}
	// serve
	p.Serve(ccs)
	for _, cc := range ccs {
		t.Logf("success create proxies with config : %v", *cc)
	}
	t.Logf("sleeping 5 seconds ...")
	time.Sleep(3 * time.Second)
}

func bootCaches(t *testing.T) {
	t.Log("skip boot by ci test controller but by travis ci environments")
}

// =========================== proxy model cases ==============

// testCommandForProxyRedisOk is the test case for standalone proxy mode redis
// 1. send all commands into the redis via raw socket
// 2. read commands by connection
// 3. parse and deal with it
func testCommandForProxyRedisOk(t *testing.T) {
	addr := "127.0.0.1:26379"
	conn, err := redis.Dial("tcp", addr)
	if assert.NoError(t, err, "fail to dial to %s", addr) {
		for _, cmd := range redisCommands {
			repl, err := conn.Do(cmd[0].(string), cmd[1:]...)
			assert.NoError(t, err, "execute command but meet error")
			// TODO: judge the reply response
			assert.NotNil(t, repl)
		}
	}
}

func testCommandForProxyRedisClusterOk(t *testing.T) {
	addr := "127.0.0.1:27000"
	conn, err := redis.Dial("tcp", addr)
	if assert.NoError(t, err, "fail to dial to %s", addr) {
		for _, cmd := range redisCommands {
			repl, err := conn.Do(cmd[0].(string), cmd[1:]...)
			assert.NoError(t, err, "execute command but meet error")
			// TODO: judge the reply response
			assert.NotNil(t, repl)
		}
	}
}

func testCommandForProxyMemcacheOk(t *testing.T) {
	mc := memcache.New("127.0.0.1:21211")

	// ==== judge basicly get/set
	key := "foo"
	data := []byte("my foo value")
	err := mc.Set(&memcache.Item{Key: key, Value: data})
	assert.NoError(t, err)
	item, err := mc.Get(key)
	assert.NoError(t, err)
	assert.Equal(t, data, item.Value)

	// ==== judge Incr/Decr
	data = []byte("1024")
	key = "bar"

	_ = mc.Set(&memcache.Item{Key: key, Value: data})
	_, _ = mc.Increment("key", uint64(1024))
	_, _ = mc.Decrement("key", uint64(10))

	item, err = mc.Get(key)
	assert.NoError(t, err)
	assert.Equal(t, []byte(fmt.Sprintf("%d", 1024+1024-10)), item.Value)

	// ==== judge basicly add
	key = "add_key"
	err = mc.Add(&memcache.Item{Key: key, Value: data})
	assert.NoError(t, err)

	item, err = mc.Get(key)
	assert.NoError(t, err)
	assert.Equal(t, []byte(fmt.Sprintf("%d", 1024)), item.Value)

	// ==== judge get mulity
	items, err := mc.GetMulti([]string{"foo", "bar", "key_not_exits"})
	assert.NoError(t, err)
	assert.Len(t, items, 2)
}

func testReconnOk(t *testing.T) {
	cancel := _createTCPProxy(t, 11211, 21220)
	defer cancel()
	// 1. try to execute with error, but click reconn process
	bs := _execute(t)
	assert.Equal(t, "SERVER_ERROR connection is closed\r\n", string(bs))
	// 2. try execute with ok
	bs = _execute(t)
	assert.Equal(t, "STORED\r\n", string(bs))
}

func testCmdNotAvaliabeNode(t *testing.T) {
	conn, err := net.DialTimeout("tcp", "127.0.0.1:26380", time.Second)
	if err != nil {
		t.Errorf("net dial error:%v", err)
	}
	defer conn.Close()
	br := bufio.NewReader(conn)
	// t.Logf("\n\nexecute cmd %s", cmd)
	cmd := []byte("*5\r\n$4\r\nmset\r\n$1\r\na\r\n$1\r\n1\r\n$1\r\nb\r\n$1\r\n2\r\n")
	conn.SetWriteDeadline(time.Now().Add(time.Second))
	if _, err = conn.Write(cmd); err != nil {
		t.Errorf("conn write cmd:%s error:%v", cmd, err)
	}
	conn.SetReadDeadline(time.Now().Add(time.Second))
	if _, err = br.ReadBytes('\n'); err != nil {
		t.Errorf("conn read cmd:%s error:%s ", cmd, err)
	}
}

func testCmdBin(t *testing.T) {
	conn, err := net.DialTimeout("tcp", "127.0.0.1:21212", time.Second)
	if err != nil {
		t.Errorf("net dial error:%v", err)
	}
	defer conn.Close()
	br := bufio.NewReader(conn)
	for _, cmd := range mcBinCommands {
		conn.SetWriteDeadline(time.Now().Add(time.Second))
		if _, err := conn.Write(cmd); err != nil {
			t.Errorf("conn write cmd:%s error:%v", cmd, err)
		}
		conn.SetReadDeadline(time.Now().Add(time.Second))
		bs := make([]byte, 24)
		if n, err := br.Read(bs); err != nil || n != 24 {
			t.Errorf("conn read cmd:%x error:%s resp:%x", cmd[1], err, bs)
			continue
		}
		if bytes.Equal(bs[6:8], []byte{0x00, 0x01}) {
			// key not found
			continue
		}
		if !bytes.Equal(bs[6:8], []byte{0x00, 0x00}) {
			t.Errorf("conn error:%s %s", bs, cmd)
			continue
		}
		bl := binary.BigEndian.Uint32(bs[8:12])
		if bl > 0 {
			body := make([]byte, bl)
			n, err := br.Read(body)
			if err != nil {
				t.Errorf("conn read body error: %v", err)
			} else if n != int(bl) {
				t.Errorf("conn read body size(%d) not equal(%d)", n, bl)
			}
		}
	}
}

func TestIntegration(t *testing.T) {
	setupTest(t)
	defer tearDownTest(t)

	t.Run("CommandForProxyRedisOk", testCommandForProxyRedisOk)
	t.Run("CommandForProxyRedisClusterOk", testCommandForProxyRedisClusterOk)
	t.Run("CommandForProxyMemcacheOk", testCommandForProxyRedisOk)
	t.Run("FeatureReconnect", testReconnOk)
	t.Run("FeatureNotAvaliableNode", testCmdNotAvaliabeNode)
}
