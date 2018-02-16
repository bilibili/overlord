package proxy

import (
	"github.com/BurntSushi/toml"
	"github.com/pkg/errors"
)

type Config struct {
	Version string
	User    string
	Dir     string
	Perf    string
	Debug   bool
	Proxy   struct {
		Proto          string
		Addr           string
		MaxConnections int  `toml:"max_connections"`
		UseMetrics     bool `toml:"use_metrics"`
	}
	Hash struct {
		Method       string
		Distribution string
		Tag          string
	}
	Cache struct {
		Type           string
		RedisAuth      string
		Timeout        int
		Preconnect     bool
		MaxConnections int  `toml:"max_connections"`
		PingPeriod     int  `toml:"ping_period"`
		PingFailLimit  int  `toml:"ping_fail_limit"`
		AutoEject      bool `toml:"auto_eject"`
		Servers        []string
	}
}

func NewDefaultConfig() *Config {
	c := &Config{}
	if _, err := toml.Decode(DefaultConfig, c); err != nil {
		panic(err)
	}
	if err := c.Validate(); err != nil {
		panic(err)
	}
	return c
}

func (c *Config) LoadFromFile(path string) error {
	_, err := toml.DecodeFile(path, c)
	if err != nil {
		return errors.Wrapf(err, "Load From File:%s", path)
	}
	return c.Validate()
}

func (c *Config) Validate() error {
	// TODO: complete validates
	return nil
}

const DefaultConfig = `
##################################################
#                                                #
#                    Overlord                    #
#        a proxy based high performance          #
#            Memcached&Redis solution            #
#                 written in Go                  #
#                                                #
##################################################
version = "0.0.1"
user = "nobody"
dir = "./"
perf = "0.0.0.0:2110"
debug = false

[proxy]
# proxy listen proto: tcp | unix
proto = "tcp"
# proxy listen addr: tcp addr | unix sock path
addr = "0.0.0.0:2111"
# proxy accept max connections from client
max_connections = 1024
# proxy support prometheus metrics
use_metrics = false

[hash]
# The name of the hash function. Possible values are: md5/crc32/fnv1a_64.
method = "fnv1a_64"
# The key distribution mode. Possible values are: ketama.
distribution = "ketama"
# A two character string that specifies the part of the key used for hashing. Eg "{}".
tag = "{}"

[cache]
# cache type: memcache | redis
type = "memcache"
# Authenticate to the Redis server on connect.
redis_auth = ""
# The timeout value in msec that we wait for to establish a connection to the server or receive a response from a server. By default, we wait indefinitely.
timeout = 1000
# A boolean value that controls if overlord should preconnect to all the servers in this pool on process start. Defaults to false.
preconnect = false
# The maximum number of connections that can be opened to each server. By default, we open at most 1 server connection.
max_connections = 1024
# Proxy will ping-pong backend memcache&redis periodly to keep-alive. Defaults to 5 second.
ping_period = 5
# The number of consecutive failures on a server that would lead to it being temporarily ejected when auto_eject is set to true. Defaults to 3.
ping_fail_limit = 3
# A boolean value that controls if server should be ejected temporarily when it fails consecutively ping_fail_limit times.
auto_eject = true
# A list of server address, port and weight (name:port:weight or ip:port:weight) for this server pool.
servers = [
    "127.0.0.1:11212:10",
    "127.0.0.1:11213:10",
    "127.0.0.1:11214:10",
    "127.0.0.1:11215:10",
    "127.0.0.1:11216:10",
]
`
