package proxy

import (
	"github.com/BurntSushi/toml"
	"github.com/felixhao/overlord/proto"
	"github.com/pkg/errors"
)

// Config proxy config.
type Config struct {
	Pprof string
	Debug bool
	Proxy struct {
		ReadTimeout    int  `toml:"read_timeout"`
		WriteTimeout   int  `toml:"write_timeout"`
		MaxConnections int  `toml:"max_connections"`
		UseMetrics     bool `toml:"use_metrics"`
	}
}

// DefaultConfig new config by defalut string.
func DefaultConfig() *Config {
	c := &Config{}
	if _, err := toml.Decode(defaultConfig, c); err != nil {
		panic(err)
	}
	if err := c.Validate(); err != nil {
		panic(err)
	}
	return c
}

// LoadFromFile load from file.
func (c *Config) LoadFromFile(path string) error {
	_, err := toml.DecodeFile(path, c)
	if err != nil {
		return errors.Wrapf(err, "Load From File:%s", path)
	}
	return c.Validate()
}

// Validate validate config field value.
func (c *Config) Validate() error {
	// TODO(felix): complete validates
	return nil
}

// ClusterConfig cluster config.
type ClusterConfig struct {
	Name             string
	HashMethod       string          `toml:"hash_method"`
	HashDistribution string          `toml:"hash_distribution"`
	HashTag          string          `toml:"hash_tag"`
	CacheType        proto.CacheType `toml:"cache_type"`
	ListenProto      string          `toml:"listen_proto"`
	ListenAddr       string          `toml:"listen_addr"`
	RedisAuth        string          `toml:"redis_auth"`
	DialTimeout      int             `toml:"dial_timeout"`
	ReadTimeout      int             `toml:"read_timeout"`
	WriteTimeout     int             `toml:"write_timeout"`
	PoolActive       int             `toml:"pool_active"`
	PoolIdle         int             `toml:"pool_idle"`
	PoolIdleTimeout  int             `toml:"pool_idle_timeout"`
	PoolGetWait      bool            `toml:"pool_get_wait"`
	PingFailLimit    int             `toml:"ping_fail_limit"`
	PingAutoEject    bool            `toml:"ping_auto_eject"`
	Servers          []string
}

// Validate validate config field value.
func (cc *ClusterConfig) Validate() error {
	// TODO(felix): complete validates
	return nil
}

// ClusterConfigs cluster configs.
type ClusterConfigs struct {
	Clusters []*ClusterConfig
}

// LoadFromFile load from file.
func (ccs *ClusterConfigs) LoadFromFile(path string) error {
	_, err := toml.DecodeFile(path, ccs)
	if err != nil {
		return errors.Wrapf(err, "Load From File:%s", path)
	}
	for _, cc := range ccs.Clusters {
		if err = cc.Validate(); err != nil {
			return err
		}
	}
	return nil
}

const defaultConfig = `
##################################################
#                                                #
#                    Overlord                    #
#        a proxy based high performance          #
#            Memcached&Redis solution            #
#                 written in Go                  #
#                                                #
##################################################
pprof = "0.0.0.0:2110"
debug = false

[proxy]
# The read timeout value in msec that we wait for to receive a response from the client. By default, we wait indefinitely.
read_timeout = 0
# The write timeout value in msec that we wait for to write a response to the client. By default, we wait indefinitely.
write_timeout = 0
# proxy accept max connections from client. By default, we no limit.
max_connections = 0
# proxy support prometheus metrics. By default, we use it.
use_metrics = true
`
