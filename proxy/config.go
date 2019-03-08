package proxy

import (
	"fmt"
	"os"
	"strconv"
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
		// it is safety to panic here
		panic(err)
	}
	if err := c.Validate(); err != nil {
		// it is safety to panic here
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
	ID               int32           // ID is set in memory, not from conf file, used to access a config from a array
	SN               int32           // SN is set in memory, not from conf file, it is used to identify the version of a config since name & ID is not changed
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

func ValidateRedisCluster(servers []string) error {
	if len(servers) == 0 {
		return errors.New("empty redis cluster list")
	}
	for _, one := range servers {
		var ipPort = strings.Split(one, ":")
		if len(ipPort) != 2 && len(ipPort) != 3 {
			return errors.New("invalid backend redis address format:" + one)
		}
		var intPort, err = strconv.Atoi(ipPort[1])
		if err != nil || intPort <= 0 {
			return errors.New("invalid backend redis address format:" + one)
		}

	}
	return nil
}

func ValidateStandalone(servers []string) error {
	if len(servers) == 0 {
		return errors.New("empty backend server list")
	}
	for _, one := range servers {
		var ipAlise = strings.Split(one, " ")
		if len(ipAlise) != 1 && len(ipAlise) != 2 {
			return errors.New("invalid backend address format:" + one)
		}
		var ipPort = strings.Split(ipAlise[0], ":")
		if len(ipPort) != 3 {
			return errors.New("invalid backend redis address format:" + one)
		}
		var intPort, err1 = strconv.Atoi(ipPort[1])
		if err1 != nil || intPort <= 0 {
			return errors.New("invalid backend redis address format:" + one)
		}
		var weight, err2 = strconv.Atoi(ipPort[2])
		if err2 != nil || weight < 0 {
			return errors.New("invalid backend redis address format:" + one)
		}
	}
	return nil
}

// Validate validate config field value.
func (cc *ClusterConfig) Validate() error {
	// TODO(felix): complete validates
	if cc.Name == "" {
		return errors.New("cluster name is not provided")
	}
	if cc.HashMethod != "fnv1a_64" {
		return errors.New("hash method:" + cc.HashMethod + " is not support")
	}
	if cc.HashDistribution != "ketama" {
		return errors.New("hash distribution:" + cc.HashDistribution + " is not support")
	}
	if cc.CacheType != types.CacheTypeMemcache && cc.CacheType != types.CacheTypeMemcacheBinary && cc.CacheType != types.CacheTypeRedis && cc.CacheType != types.CacheTypeRedisCluster {
		return errors.New("back end cache type is not support")
	}
	if cc.ListenProto != "tcp" && cc.ListenProto != "unix" {
		return errors.New("listen proto:" + cc.ListenProto + " is not support")
	}
	if cc.DialTimeout < 0 || cc.ReadTimeout < 0 || cc.WriteTimeout < 0 || cc.NodeConnections <= 0 || cc.PingFailLimit < 0 {
		return errors.New("cannot meet condition: dial_timeout >= 0 and read_timeout >= 0 write_timeout >= 0 && node_connections > 0 && ping_fail_limit > 0")
	}

	var ipPort = strings.Split(cc.ListenAddr, ":")
	if len(ipPort) != 2 {
		return errors.New("invalid listen address:" + cc.ListenAddr)
	}
	var intPort, err = strconv.Atoi(ipPort[1])
	if err != nil {
		return errors.New("invalid listen address:" + cc.ListenAddr)
	}
	if intPort <= 0 {
		return errors.New("invalid port in listen address:" + cc.ListenAddr)
	}
	if cc.CacheType == types.CacheTypeRedisCluster {
		err = ValidateRedisCluster(cc.Servers)
		if err != nil {
			return err
		}
	} else {
		err = ValidateStandalone(cc.Servers)
		if err != nil {
			return err
		}
	}
	return nil
}

// SetDefault config content with cluster config
func (cc *ClusterConfig) SetDefault() {
	if len(cc.Servers) == 0 {
		return
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

func LoadClusterConf(path string) (ccs []*ClusterConfig, err error) {
	err = nil
	ccs = nil
	checks := map[string]struct{}{}
	cs := &ClusterConfigs{}
	if err = cs.LoadFromFile(path); err != nil {
		return
	}
	for _, cc := range cs.Clusters {
		if _, ok := checks[cc.Name]; ok {
			err = errors.New("duplicate cluster name:" + cc.Name + " in cluster conf file.")
			return
		}
		checks[cc.Name] = struct{}{}
		var ipPort = strings.Split(cc.ListenAddr, ":")
		if len(ipPort) != 2 {
			err = errors.New("invalid listen address:" + cc.ListenAddr + " in cluster conf file.")
			return
		}
		var port = ipPort[1]
		if _, ok := checks[port]; ok {
			err = errors.New("duplicate listen address:" + cc.ListenAddr + " in cluster conf file.")
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
