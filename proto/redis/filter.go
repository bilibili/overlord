package redis

import (
	"bytes"

	"github.com/tatsushid/go-critbit"
)

// reqType is a kind for cmd which is Ctl/Read/Write/NotSupport
type reqType = int

const (
	reqTypeCtl        reqType = iota
	reqTypeRead       reqType = iota
	reqTypeWrite      reqType = iota
	reqTypeNotSupport reqType = iota
)

var reqMap *critbit.Tree

func init() {

	tmpMap := map[string]reqType{
		"DEL":       reqTypeWrite,
		"DUMP":      reqTypeRead,
		"EXISTS":    reqTypeRead,
		"EXPIRE":    reqTypeWrite,
		"EXPIREAT":  reqTypeWrite,
		"KEYS":      reqTypeNotSupport,
		"MIGRATE":   reqTypeNotSupport,
		"MOVE":      reqTypeNotSupport,
		"OBJECT":    reqTypeNotSupport,
		"PERSIST":   reqTypeWrite,
		"PEXPIRE":   reqTypeWrite,
		"PEXPIREAT": reqTypeWrite,
		"PTTL":      reqTypeRead,

		"RANDOMKEY":   reqTypeNotSupport,
		"RENAME":      reqTypeNotSupport,
		"RENAMENX":    reqTypeNotSupport,
		"RESTORE":     reqTypeWrite,
		"SCAN":        reqTypeNotSupport,
		"SORT":        reqTypeWrite,
		"TTL":         reqTypeRead,
		"TYPE":        reqTypeRead,
		"WAIT":        reqTypeNotSupport,
		"APPEND":      reqTypeWrite,
		"BITCOUNT":    reqTypeRead,
		"BITOP":       reqTypeNotSupport,
		"BITPOS":      reqTypeRead,
		"DECR":        reqTypeWrite,
		"DECRBY":      reqTypeWrite,
		"GET":         reqTypeRead,
		"GETBIT":      reqTypeRead,
		"GETRANGE":    reqTypeRead,
		"GETSET":      reqTypeWrite,
		"INCR":        reqTypeWrite,
		"INCRBY":      reqTypeWrite,
		"INCRBYFLOAT": reqTypeWrite,

		"MGET":     reqTypeRead,
		"MSET":     reqTypeWrite,
		"MSETNX":   reqTypeNotSupport,
		"PSETEX":   reqTypeWrite,
		"SET":      reqTypeWrite,
		"SETBIT":   reqTypeWrite,
		"SETEX":    reqTypeWrite,
		"SETNX":    reqTypeWrite,
		"SETRANGE": reqTypeWrite,
		"STRLEN":   reqTypeRead,

		"HDEL":         reqTypeWrite,
		"HEXISTS":      reqTypeRead,
		"HGET":         reqTypeRead,
		"HGETALL":      reqTypeRead,
		"HINCRBY":      reqTypeWrite,
		"HINCRBYFLOAT": reqTypeWrite,
		"HKEYS":        reqTypeRead,
		"HLEN":         reqTypeRead,
		"HMGET":        reqTypeRead,
		"HMSET":        reqTypeWrite,
		"HSET":         reqTypeWrite,
		"HSETNX":       reqTypeWrite,
		"HSTRLEN":      reqTypeRead,
		"HVALS":        reqTypeRead,
		"HSCAN":        reqTypeRead,
		"BLPOP":        reqTypeNotSupport,
		"BRPOP":        reqTypeNotSupport,
		"BRPOPLPUSH":   reqTypeNotSupport,

		"LINDEX":    reqTypeRead,
		"LINSERT":   reqTypeWrite,
		"LLEN":      reqTypeRead,
		"LPOP":      reqTypeWrite,
		"LPUSH":     reqTypeWrite,
		"LPUSHX":    reqTypeWrite,
		"LRANGE":    reqTypeRead,
		"LREM":      reqTypeWrite,
		"LSET":      reqTypeWrite,
		"LTRIM":     reqTypeWrite,
		"RPOP":      reqTypeWrite,
		"RPOPLPUSH": reqTypeWrite,
		"RPUSH":     reqTypeWrite,
		"RPUSHX":    reqTypeWrite,

		"SADD":        reqTypeWrite,
		"SCARD":       reqTypeRead,
		"SDIFF":       reqTypeRead,
		"SDIFFSTORE":  reqTypeWrite,
		"SINTER":      reqTypeRead,
		"SINTERSTORE": reqTypeWrite,
		"SISMEMBER":   reqTypeRead,
		"SMEMBERS":    reqTypeRead,
		"SMOVE":       reqTypeWrite,
		"SPOP":        reqTypeWrite,
		"SRANDMEMBER": reqTypeRead,
		"SREM":        reqTypeWrite,
		"SUNION":      reqTypeRead,
		"SUNIONSTORE": reqTypeWrite,
		"SSCAN":       reqTypeRead,

		"ZADD":             reqTypeWrite,
		"ZCARD":            reqTypeRead,
		"ZCOUNT":           reqTypeRead,
		"ZINCRBY":          reqTypeWrite,
		"ZINTERSTORE":      reqTypeWrite,
		"ZLEXCOUNT":        reqTypeRead,
		"ZRANGE":           reqTypeRead,
		"ZRANGEBYLEX":      reqTypeRead,
		"ZRANGEBYSCORE":    reqTypeRead,
		"ZRANK":            reqTypeRead,
		"ZREM":             reqTypeWrite,
		"ZREMRANGEBYLEX":   reqTypeWrite,
		"ZREMRANGEBYRANK":  reqTypeWrite,
		"ZREMRANGEBYSCORE": reqTypeWrite,
		"ZREVRANGE":        reqTypeRead,
		"ZREVRANGEBYLEX":   reqTypeRead,
		"ZREVRANGEBYSCORE": reqTypeRead,
		"ZREVRANK":         reqTypeRead,
		"ZSCORE":           reqTypeRead,
		"ZUNIONSTORE":      reqTypeWrite,
		"ZSCAN":            reqTypeRead,

		"PFADD":   reqTypeWrite,
		"PFCOUNT": reqTypeRead,
		"PFMERGE": reqTypeWrite,
		"EVAL":    reqTypeNotSupport,
		"EVALSHA": reqTypeNotSupport,
		"PING":    reqTypeNotSupport,
		"AUTH":    reqTypeNotSupport,
		"ECHO":    reqTypeNotSupport,
		"INFO":    reqTypeNotSupport,
		"PROXY":   reqTypeNotSupport,
		"SLOWLOG": reqTypeNotSupport,
		"QUIT":    reqTypeNotSupport,
		"SELECT":  reqTypeNotSupport,
		"TIME":    reqTypeNotSupport,
		"CONFIG":  reqTypeNotSupport,
	}

	reqMap = critbit.New()
	for key, tp := range tmpMap {
		reqMap.Insert([]byte(key), tp)
	}
}

func getReqType(cmd []byte) reqType {
	idx := bytes.IndexByte(cmd, lfByte)
	if idx != -1 {
		cmd = cmd[idx+1:]
	}

	val, ok := reqMap.Get(cmd)
	if !ok {
		return reqTypeNotSupport
	}
	return val.(int)
}
