package redis

import (
	"bytes"
)

// Command type
// MSET split with command change command as SET
// EXISTS will return an existed count
// DELETE will return delete count
// CLUSTER NODES will return an mock response of cluster

// CmdType is the type of proxy
type CmdType = uint8

// command types for read/write spliting
const (
	CmdTypeRead uint8 = iota
	CmdTypeWrite
	CmdTypeNotSupport
	CmdTypeCtl
)

var cmdTypeMap = map[string]CmdType{
	"DEL":              CmdTypeWrite,
	"DUMP":             CmdTypeRead,
	"EXISTS":           CmdTypeRead,
	"EXPIRE":           CmdTypeWrite,
	"EXPIREAT":         CmdTypeWrite,
	"KEYS":             CmdTypeNotSupport,
	"MIGRATE":          CmdTypeNotSupport,
	"MOVE":             CmdTypeNotSupport,
	"OBJECT":           CmdTypeNotSupport,
	"PERSIST":          CmdTypeWrite,
	"PEXPIRE":          CmdTypeWrite,
	"PEXPIREAT":        CmdTypeWrite,
	"PTTL":             CmdTypeRead,
	"RANDOMKEY":        CmdTypeNotSupport,
	"RENAME":           CmdTypeNotSupport,
	"RENAMENX":         CmdTypeNotSupport,
	"RESTORE":          CmdTypeWrite,
	"SCAN":             CmdTypeNotSupport,
	"SORT":             CmdTypeWrite,
	"TTL":              CmdTypeRead,
	"TYPE":             CmdTypeRead,
	"WAIT":             CmdTypeNotSupport,
	"APPEND":           CmdTypeWrite,
	"BITCOUNT":         CmdTypeRead,
	"BITOP":            CmdTypeNotSupport,
	"BITPOS":           CmdTypeRead,
	"DECR":             CmdTypeWrite,
	"DECRBY":           CmdTypeWrite,
	"GET":              CmdTypeRead,
	"GETBIT":           CmdTypeRead,
	"GETRANGE":         CmdTypeRead,
	"GETSET":           CmdTypeWrite,
	"INCR":             CmdTypeWrite,
	"INCRBY":           CmdTypeWrite,
	"INCRBYFLOAT":      CmdTypeWrite,
	"MGET":             CmdTypeRead,
	"MSET":             CmdTypeWrite,
	"MSETNX":           CmdTypeNotSupport,
	"PSETEX":           CmdTypeWrite,
	"SET":              CmdTypeWrite,
	"SETBIT":           CmdTypeWrite,
	"SETEX":            CmdTypeWrite,
	"SETNX":            CmdTypeWrite,
	"SETRANGE":         CmdTypeWrite,
	"STRLEN":           CmdTypeRead,
	"HDEL":             CmdTypeWrite,
	"HEXISTS":          CmdTypeRead,
	"HGET":             CmdTypeRead,
	"HGETALL":          CmdTypeRead,
	"HINCRBY":          CmdTypeWrite,
	"HINCRBYFLOAT":     CmdTypeWrite,
	"HKEYS":            CmdTypeRead,
	"HLEN":             CmdTypeRead,
	"HMGET":            CmdTypeRead,
	"HMSET":            CmdTypeWrite,
	"HSET":             CmdTypeWrite,
	"HSETNX":           CmdTypeWrite,
	"HSTRLEN":          CmdTypeRead,
	"HVALS":            CmdTypeRead,
	"HSCAN":            CmdTypeRead,
	"BLPOP":            CmdTypeNotSupport,
	"BRPOP":            CmdTypeNotSupport,
	"BRPOPLPUSH":       CmdTypeNotSupport,
	"LINDEX":           CmdTypeRead,
	"LINSERT":          CmdTypeWrite,
	"LLEN":             CmdTypeRead,
	"LPOP":             CmdTypeWrite,
	"LPUSH":            CmdTypeWrite,
	"LPUSHX":           CmdTypeWrite,
	"LRANGE":           CmdTypeRead,
	"LREM":             CmdTypeWrite,
	"LSET":             CmdTypeWrite,
	"LTRIM":            CmdTypeWrite,
	"RPOP":             CmdTypeWrite,
	"RPOPLPUSH":        CmdTypeWrite,
	"RPUSH":            CmdTypeWrite,
	"RPUSHX":           CmdTypeWrite,
	"SADD":             CmdTypeWrite,
	"SCARD":            CmdTypeRead,
	"SDIFF":            CmdTypeRead,
	"SDIFFSTORE":       CmdTypeWrite,
	"SINTER":           CmdTypeRead,
	"SINTERSTORE":      CmdTypeWrite,
	"SISMEMBER":        CmdTypeRead,
	"SMEMBERS":         CmdTypeRead,
	"SMOVE":            CmdTypeWrite,
	"SPOP":             CmdTypeWrite,
	"SRANDMEMBER":      CmdTypeRead,
	"SREM":             CmdTypeWrite,
	"SUNION":           CmdTypeRead,
	"SUNIONSTORE":      CmdTypeWrite,
	"SSCAN":            CmdTypeRead,
	"ZADD":             CmdTypeWrite,
	"ZCARD":            CmdTypeRead,
	"ZCOUNT":           CmdTypeRead,
	"ZINCRBY":          CmdTypeWrite,
	"ZINTERSTORE":      CmdTypeWrite,
	"ZLEXCOUNT":        CmdTypeRead,
	"ZRANGE":           CmdTypeRead,
	"ZRANGEBYLEX":      CmdTypeRead,
	"ZRANGEBYSCORE":    CmdTypeRead,
	"ZRANK":            CmdTypeRead,
	"ZREM":             CmdTypeWrite,
	"ZREMRANGEBYLEX":   CmdTypeWrite,
	"ZREMRANGEBYRANK":  CmdTypeWrite,
	"ZREMRANGEBYSCORE": CmdTypeWrite,
	"ZREVRANGE":        CmdTypeRead,
	"ZREVRANGEBYLEX":   CmdTypeRead,
	"ZREVRANGEBYSCORE": CmdTypeRead,
	"ZREVRANK":         CmdTypeRead,
	"ZSCORE":           CmdTypeRead,
	"ZUNIONSTORE":      CmdTypeWrite,
	"ZSCAN":            CmdTypeRead,
	"PFADD":            CmdTypeWrite,
	"PFCOUNT":          CmdTypeRead,
	"PFMERGE":          CmdTypeWrite,
	"EVAL":             CmdTypeWrite,
	"EVALSHA":          CmdTypeNotSupport,

	"PING":    CmdTypeRead,
	"AUTH":    CmdTypeNotSupport,
	"ECHO":    CmdTypeNotSupport,
	"INFO":    CmdTypeNotSupport,
	"PROXY":   CmdTypeNotSupport,
	"SLOWLOG": CmdTypeNotSupport,
	"QUIT":    CmdTypeNotSupport,
	"SELECT":  CmdTypeNotSupport,
	"TIME":    CmdTypeNotSupport,
	"CONFIG":  CmdTypeNotSupport,
}

func getCmdType(cmd string) CmdType {
	if ctype, ok := cmdTypeMap[cmd]; ok {
		return ctype
	}
	return CmdTypeNotSupport
}

// MergeType is used to decript the merge operation.
type MergeType = uint8

// merge types
const (
	MergeTypeCount MergeType = iota
	MergeTypeOk
	MergeTypeJoin
	MergeTypeBasic
)

func getMergeType(cmd []byte) MergeType {
	// TODO: impl with tire tree to search quickly
	if bytes.Equal(cmd, cmdMGetBytes) {
		return MergeTypeJoin
	}
	if bytes.Equal(cmd, cmdMSetBytes) {
		return MergeTypeOk
	}

	if bytes.Equal(cmd, cmdExistsBytes) || bytes.Equal(cmd, cmdDelBytes) {
		return MergeTypeCount
	}

	return MergeTypeBasic
}
