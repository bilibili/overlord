package proto

import (
	errs "errors"
)

// errors
var (
	ErrNoSupportCacheType = errs.New("unsupported cache type")
)

// CacheType memcache or redis
type CacheType string

// Cache type: memcache or redis.
const (
	CacheTypeUnknown  CacheType = "unknown"
	CacheTypeMemcache CacheType = "memcache"
	CacheTypeRedis    CacheType = "redis"
)

// Request request interface.
type Request interface {
	Cmd() string
	Key() []byte
	Resp() []byte
}

// ProxyConn decode bytes from client and encode write to conn.
type ProxyConn interface {
	Read() error
	Decode(msg *Message) (completed bool, err error)
	Encode(msg *Message) error
}

// NodeConn handle Msg to backend cache server and read response.
type NodeConn interface {
	Write(*Message) error
	Read(*Message) error
	Ping() error
	Close() error
}
