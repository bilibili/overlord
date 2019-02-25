package proxy

import (
	"fmt"
	"strings"

	"overlord/pkg/log"
	"overlord/pkg/types"

	"github.com/BurntSushi/toml"
	"github.com/pkg/errors"
)

// Config proxy config.
type Config struct {
	Pprof string
	*log.Config
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
    ID               int32           // ID is set in memory, not from conf file
	HashMethod       string          `toml:"hash_method"`
	HashDistribution string          `toml:"hash_distribution"`
	HashTag          string          `toml:"hash_tag"`
	CacheType        types.CacheType `toml:"cache_type"`
	ListenProto      string          `toml:"listen_proto"`
	ListenAddr       string          `toml:"listen_addr"`
	RedisAuth        string          `toml:"redis_auth"`
	DialTimeout      int             `toml:"dial_timeout"`
	ReadTimeout      int             `toml:"read_timeout"`
	WriteTimeout     int             `toml:"write_timeout"`
	NodeConnections  int32           `toml:"node_connections"`
	PingFailLimit    int             `toml:"ping_fail_limit"`
	PingAutoEject    bool            `toml:"ping_auto_eject"`
	CloseWhenChange  bool            `toml:"close_front_conn_when_conf_change"`
	Servers          []string        `toml:"servers"`
}

// Validate validate config field value.
func (cc *ClusterConfig) Validate() error {
	// TODO(felix): complete validates
    if (cc.HashMethod != "fnv1a_64") {
        return errors.New("hash method:" + cc.HashMethod + " is not support")
    }
    if (cc.HashDistribution  != "ketama") {
        return errors.New("hash distribution:" + cc.HashDistribution + " is not support")
    }
    if (cc.CacheType != types.CacheTypeMemcache && cc.CacheType != types.CacheTypeMemcacheBinary && cc.CacheType != types.CacheTypeRedis && cc.CacheType != types.CacheTypeRedisCluster) {
        return errors.New("back end cache type is not support")
    }
    if (cc.ListenProto != "tcp" && cc.ListenProto != "unix") {
        return errors.New("listen proto:" + cc.ListenProto + " is not support")
    }
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
        cc.CloseWhenChange = false
		if cc.CacheType == types.CacheTypeRedisCluster {
			servers := make([]string, len(cc.Servers))
			for i, server := range cc.Servers {
				ssp := strings.Split(server, ":")
				if len(ssp) == 3 {
					servers[i] = fmt.Sprintf("%s:%s", ssp[0], ssp[1])
				} else {
					servers[i] = server
				}
			}
			cc.Servers = servers
		}
	}
	return nil
}

func LoadClusterConf(path string) (succ bool, msg string, ccs []*ClusterConfig) {
	checks := map[string]struct{}{}
    cs := &ClusterConfigs{}
    if err := cs.LoadFromFile(path); err != nil {
        succ = false
        msg = "failed to load cluster conf file:" + path
        return
    }
    for _, cc := range cs.Clusters {
        if _, ok := checks[cc.Name]; ok {
            succ = false
            msg = "duplicate cluster name:" + cc.Name + " in cluster conf file."
            return
        }
        checks[cc.Name] = struct{}{}
        var ipPort = strings.Split(cc.ListenAddr, ":")
        if len(ipPort) != 2 {
            succ = false
            msg = "invalid listen address:" + cc.ListenAddr + " in cluster conf file."
            return
        }
        var port = ipPort[1]
        if _, ok := checks[port]; ok {
            succ = false
            msg = "duplicate listen address:" + cc.ListenAddr + " in cluster conf file."
            return
        }
        checks[port] = struct{}{}
    }
    succ = true
    msg = ""
    ccs = append(ccs, cs.Clusters...)
    return
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
