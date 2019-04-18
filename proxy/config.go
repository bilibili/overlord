package proxy

import (
	errs "errors"
	"fmt"
	"os"
	"strconv"
	"strings"

	"overlord/pkg/log"
	"overlord/pkg/types"

	"github.com/BurntSushi/toml"
	"github.com/Pallinder/go-randomdata"
	"github.com/pkg/errors"
)

// errs
var (
	ErrClusterConfInvalid   = errs.New("cluster config is invalid")
	ErrClusterConfDuplicate = errs.New("cluster config is duplicate")
)

// Config proxy config.
type Config struct {
	Stat string
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
	Name              string
	HashMethod        string          `toml:"hash_method"`
	HashDistribution  string          `toml:"hash_distribution"`
	HashTag           string          `toml:"hash_tag"`
	CacheType         types.CacheType `toml:"cache_type"`
	ListenProto       string          `toml:"listen_proto"`
	ListenAddr        string          `toml:"listen_addr"`
	RedisAuth         string          `toml:"redis_auth"`
	DialTimeout       int             `toml:"dial_timeout"`
	ReadTimeout       int             `toml:"read_timeout"`
	WriteTimeout      int             `toml:"write_timeout"`
	NodeConnections   int32           `toml:"node_connections"`
	PingFailLimit     int             `toml:"ping_fail_limit"`
	PingAutoEject     bool            `toml:"ping_auto_eject"`
	SlowlogSlowerThan int             `toml:"slowlog_slower_than"`
	Servers           []string        `toml:"servers"`
}

// ValidateStandalone validate redis/memcache address is valid or not
func ValidateStandalone(servers []string) (err error) {
	if len(servers) == 0 {
		return errs.New("empty backend server list")
	}
	var hasAlias bool
	for i, server := range servers {
		ipAlias := strings.Split(server, " ")
		if i == 0 && len(ipAlias) == 2 {
			hasAlias = true
		}
		if (hasAlias && len(ipAlias) != 2) || (!hasAlias && len(ipAlias) != 1) {
			err = errors.Wrapf(ErrClusterConfInvalid, "server:%s", server)
			return
		}
		ipPort := strings.Split(ipAlias[0], ":")
		if len(ipPort) != 3 {
			err = errors.Wrapf(ErrClusterConfInvalid, "server:%s", server)
			return
		}
		if port, e := strconv.Atoi(ipPort[1]); e != nil || port <= 0 {
			err = errors.Wrapf(ErrClusterConfInvalid, "server:%s", server)
			return
		}
		if weight, e := strconv.Atoi(ipPort[2]); e != nil || weight < 0 {
			err = errors.Wrapf(ErrClusterConfInvalid, "server:%s", server)
			return
		}
	}
	return
}

// Validate validate config field value.
func (cc *ClusterConfig) Validate() error {
	// TODO(felix): complete validates
	if cc.CacheType != types.CacheTypeRedisCluster {
		return ValidateStandalone(cc.Servers)
	}
	return nil
}

// SetDefault config content with cluster config
func (cc *ClusterConfig) SetDefault() {
	if len(cc.Servers) == 0 {
		return
	}
	if cc.Name == "" {
		cc.Name = randomdata.SillyName()
	}

	if cc.HashMethod == "" {
		cc.HashMethod = "fnv1a64"
	}

	if cc.HashDistribution == "" {
		cc.HashDistribution = "ketama"
	}

	if cc.HashTag == "" {
		cc.HashTag = "{}"
	}

	if cc.ListenProto == "" {
		cc.ListenProto = "tcp"
	}

	if cc.NodeConnections == 0 {
		cc.NodeConnections = 2
	}

	if len(cc.ListenAddr) == 0 {
		fmt.Fprint(os.Stderr, "checking out ListenAddr may only using for [anzi] from\n")
	} else if !strings.Contains(cc.ListenAddr, ":") {
		addr := fmt.Sprintf("%s:%s", "0.0.0.0", cc.ListenAddr)
		fmt.Fprintf(os.Stderr, "cluster(%s).cc.ListenAddr don't contains ':', using %s\n", cc.Name, addr)
		cc.ListenAddr = addr
	}
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
		cc.SetDefault()
		if err = cc.Validate(); err != nil {
			return err
		}
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

// LoadClusterConf load cluster config.
func LoadClusterConf(path string) (ccs []*ClusterConfig, err error) {
	cs := &ClusterConfigs{}
	if err = cs.LoadFromFile(path); err != nil {
		return
	}
	checks := map[string]struct{}{}
	for _, cc := range cs.Clusters {
		if _, ok := checks[cc.Name]; ok {
			err = errors.Wrapf(ErrClusterConfDuplicate, "name:%s", cc.Name)
			return
		}
		checks[cc.Name] = struct{}{}
		ipPort := strings.Split(cc.ListenAddr, ":")
		if len(ipPort) != 2 {
			err = errors.Wrapf(ErrClusterConfInvalid, "addr:%s", cc.ListenAddr)
			return
		}
		port := ipPort[1]
		if _, ok := checks[port]; ok {
			err = errors.Wrapf(ErrClusterConfDuplicate, "addr:%s", cc.ListenAddr)
			return
		}
		checks[port] = struct{}{}
	}
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
