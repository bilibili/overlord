package proto

import (
	errs "errors"
)

// errors
var (
	ErrNoSupportCacheType = errs.New("unsupported cache type")
)

// CacheType memcache or redis
type CacheType = string

// Cache type: memcache or redis.
const (
	CacheTypeUnknown        CacheType = "unknown"
	CacheTypeMemcache       CacheType = "memcache"
	CacheTypeMemcacheBinary CacheType = "memcache_binary"
	CacheTypeRedis          CacheType = "redis"
	CacheTypeRedisCluster   CacheType = "redis_cluster"
)

// Request request interface.
type Request interface {
	CmdString() string
	Cmd() []byte
	Key() []byte
	Put()
}

// ProxyConn decode bytes from client and encode write to conn.
type ProxyConn interface {
	Decode([]*Message) ([]*Message, error)
	Encode(msg *Message) error
	Flush() error
}

// NodeConn handle Msg to backend cache server and read response.
type NodeConn interface {
	WriteBatch(*MsgBatch) error
	ReadBatch(*MsgBatch) error

	Ping() error
	Close() error
}

// RedisClusterNodeConn handle Msg to backend cache server and read response.
type RedisClusterNodeConn interface {
	NodeConn
}

// Executor is the interface for backend run and execute the messages.
type Executor interface {
	Execute(mba *MsgBatchAllocator, msgs []*Message) error
}
