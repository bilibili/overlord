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
	CacheTypeUnknown        CacheType = "unknown"
	CacheTypeMemcache       CacheType = "memcache"
	CacheTypeMemcacheBinary CacheType = "memcache_binary"
	CacheTypeRedis          CacheType = "redis"
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
	Close() error
}

// Pinger for executor ping node.
type Pinger interface {
	Ping() error
	Close() error
}

// Forwarder is the interface for backend run and process the messages.
type Forwarder interface {
	Forward(mba *MsgBatchAllocator, msgs []*Message) error
	Close() error
}
