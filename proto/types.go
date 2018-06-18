package proto

import (
	errs "errors"
)

// errors
var (
	// ErrMoreData           = errs.New("need more data")
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
	// IsBatch() bool
	// Batch() []Message
	// Merge() [][]byte
}

// ProxyConn decode bytes from client and encode write to conn.
type ProxyConn interface {
	Decode() (*Message, error)
	Encode(*Message) error
}

// NodeConn handle Msg to backend cache server and read response.
type NodeConn interface {
	Write(*Message) error
	Read(*Message) error
	Close() error
}

// Pinger ping node connection.
type Pinger interface {
	Ping() error
	Close() error
}
