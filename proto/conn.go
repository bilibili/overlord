package proto

import (
	"errors"
	"net"
	"time"
)

// errors
var (
	ErrNoSupportCacheType = errors.New("no support cache type")
)

type cacheType int8

// Cache type: memcache or redis.
const (
	CacheMemcache cacheType = iota
	CacheRedis
)

type Encoder interface{}
type Decoder interface{}

// Conn is proto conn.
type Conn struct {
	conn net.Conn

	Encoder
	Decoder

	ReadTimeout  time.Duration
	WriteTimeout time.Duration
}

// NewConn new a proto Conn and distinguish by cache type.
func NewConn(conn net.Conn, cacheType cacheType) *Conn {
	c := &Conn{conn: conn}
	if cacheType == CacheMemcache {
		// c.Encoder =

	} else if cacheType == CacheRedis {
		// TODO: support redis
	} else {
		panic(ErrNoSupportCacheType)
	}
	return c
}
