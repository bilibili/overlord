package types

import "errors"

// CacheType memcache or redis
type CacheType string

// errors
var (
	ErrNoSupportCacheType = errors.New("unsupported cache type")
)

// Cache type: memcache or redis.
const (
	CacheTypeUnknown        CacheType = "unknown"
	CacheTypeMemcache       CacheType = "memcache"
	CacheTypeMemcacheBinary CacheType = "memcache_binary"
	CacheTypeRedis          CacheType = "redis"
	CacheTypeRedisCluster   CacheType = "redis_cluster"
)
