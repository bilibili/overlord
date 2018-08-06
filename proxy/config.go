package proxy

import (
	"overlord/proto"
	rc "overlord/proto/redis/cluster"

	"time"

	"github.com/BurntSushi/toml"
	"github.com/pkg/errors"
)

// Config proxy config.
type Config struct {
	Pprof string
	Debug bool
	Log   string
	LogVL int `toml:"log_vl"`
	Proxy struct {
		ReadTimeout    int   `toml:"read_timeout"`
		WriteTimeout   int   `toml:"write_timeout"`
		MaxConnections int32 `toml:"max_connections"`
		UseMetrics     bool  `toml:"use_metrics"`
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
	NodeConnections  int32           `toml:"node_connections"`
	PingFailLimit    int             `toml:"ping_fail_limit"`
	PingAutoEject    bool            `toml:"ping_auto_eject"`
	Servers          []string
}

// AsRedisClusterConfig convert self into RedisClusterConfig to avoid cycle import.
func (cc *ClusterConfig) AsRedisClusterConfig() *rc.RedisClusterConfig {
	return &rc.RedisClusterConfig{
		Cluster: cc.Name,
		Servers: cc.Servers,
		HashTag: []byte(cc.HashTag),

		NodeConnections: cc.NodeConnections,

		ReadTimeout:  time.Millisecond * time.Duration(cc.ReadTimeout),
		WriteTimeout: time.Millisecond * time.Duration(cc.WriteTimeout),
		DialTimeout:  time.Millisecond * time.Duration(cc.DialTimeout),
	}
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
log = ""
log_lv = 0

[proxy]
# The read timeout value in msec that we wait for to receive a response from the client. By default, we wait indefinitely.
read_timeout = 0
# The write timeout value in msec that we wait for to write a response to the client. By default, we wait indefinitely.
write_timeout = 0
# proxy accept max connections from client. By default, we no limit.
max_connections = 0
# proxy support prometheus metrics, reuse the pprof port. By default, we use it.
use_metrics = true
`
