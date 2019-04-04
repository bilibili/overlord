package reload

import (
	"github.com/bradfitz/gomemcache/memcache"
	"github.com/gomodule/redigo/redis"
	"github.com/stretchr/testify/assert"

	"overlord/proxy"

	"testing"
	"time"

	"fmt"
	"sync"
)

var (
	redisPorts = []int{9001, 9002}
	mcPorts    = []int{9101, 9102}

	key       = "my-key"
	redisPort = 20001
	mcPort    = 20002

	clusterConfFile = "./example/proxy.toml"
	ccf0            = "./conf.d/mc0.toml"
	ccf1            = "./conf.d/mc1.toml"

	ccf2 = "./conf.d/redis2.toml"
	ccf3 = "./conf.d/redis3.toml"

	once sync.Once
)

func setupTest(t *testing.T, clusterConfFile string) {
	once.Do(func() {
		assert.NoError(t, cp(ccf0, clusterConfFile))

		p, err := proxy.New(proxy.DefaultConfig())
		if assert.NoError(t, err) {
			ccs, err := proxy.LoadClusterConf(clusterConfFile)
			if assert.NoError(t, err) {
				// serve
				p.Serve(ccs)
				for _, cc := range ccs {
					t.Logf("success create proxies with config : %v", *cc)
				}
				t.Logf("sleeping 5 seconds ...")
				time.Sleep(3 * time.Second)
			}
		}
	})
}

func teardownTest(t *testing.T) {
	assert.NoError(t, cp(ccf0, clusterConfFile))
	time.Sleep(time.Second)
}

// TestReloadWithProxyMemcacheOk ...
func TestReloadWithProxyMemcacheOk(t *testing.T) {
	setupTest(t, clusterConfFile)
	defer teardownTest(t)

	mc0 := memcache.New(fmt.Sprintf("127.0.0.1:%d", mcPorts[0]))
	mc1 := memcache.New(fmt.Sprintf("127.0.0.1:%d", mcPorts[1]))

	_ = mc0.Delete(key)
	_ = mc1.Delete(key)

	assert.NoError(t, mc0.Set(&memcache.Item{Key: key, Value: []byte("i am mc0")}))
	assert.NoError(t, mc1.Set(&memcache.Item{Key: key, Value: []byte("i am mc1")}))

	mcs := []*memcache.Client{mc0, mc1}
	values := []string{"i am mc0", "i am mc1"}
	ccfs := []string{ccf0, ccf1}

	for i := 0; i < 4; i++ {
		now := i % 2
		next := (i + 1) % 2
		name := fmt.Sprintf("Iter%d_From%d_To%d", i, now, next)
		if i == 0 {
			name = "IterReloadNoChange"
		}

		t.Run(name, func(t *testing.T) {
			assert.NoError(t, cp(ccfs[next], clusterConfFile))
			time.Sleep(time.Second)
			item, err := mcs[next].Get(key)
			assert.NoError(t, err)
			if assert.NotNil(t, item) && assert.NotNil(t, item.Value) {
				assert.Equal(t, values[next], string(item.Value))
			}
		})
	}
}

// TestReloadWithProxyRedisOk ...
func TestReloadWithProxyRedisOk(t *testing.T) {
	setupTest(t, clusterConfFile)
	defer teardownTest(t)

	rc0, err := redis.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", redisPorts[0]))
	assert.NoError(t, err)
	rc1, err := redis.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", redisPorts[1]))
	assert.NoError(t, err)

	_, err = rc0.Do("DEL", key)
	assert.NoError(t, err)

	_, err = rc1.Do("DEL", key)
	assert.NoError(t, err)

	_, err = rc0.Do("SET", key, "i am rc0")
	assert.NoError(t, err)
	_, err = rc1.Do("SET", key, "i am rc1")
	assert.NoError(t, err)

	rcs := []redis.Conn{rc0, rc1}
	values := []string{"i am rc0", "i am rc1"}
	ccfs := []string{ccf2, ccf3}

	for i := 0; i < 4; i++ {
		now := i % 2
		next := (i + 1) % 2
		name := fmt.Sprintf("Iter%d_From%d_To%d", i, now, next)
		if i == 0 {
			name = "IterReloadNoChange"
		}
		t.Run(name, func(t *testing.T) {
			assert.NoError(t, cp(ccfs[next], clusterConfFile))
			time.Sleep(time.Second)

			reply, err := redis.Bytes(rcs[next].Do("GET", key))
			assert.NoError(t, err)
			assert.Equal(t, values[next], string(reply))
		})
	}
}

func TestClusterConfigInvalidConf(t *testing.T) {
	setupTest(t, clusterConfFile)

	confs := []string{
		"empty_server.conf", "more_alisa.conf", "no_weight.conf", "invalid_port.conf",
		"invalid_weight.conf", "duplicate_name.conf", "duplicate_ip.conf", "some_has_alisa.conf"}

	for _, cf := range confs {
		cfp := fmt.Sprintf("./conf.d/invalid/%s", cf)
		t.Run(fmt.Sprintf("FileName_%s", cf), func(t *testing.T) {
			_, err := proxy.LoadClusterConf(cfp)
			assert.Error(t, err, "load conf get error:%s\n", err.Error())
		})
	}
}
