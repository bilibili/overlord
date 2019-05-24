package anzi

import (
	"bufio"
	"fmt"
	"net"
	"strconv"

	"overlord/pkg/log"
)

// define useful command
var (
	BytesSet = []byte("SET")

	BytesSAdd  = []byte("SADD")
	BytesRPush = []byte("RPUSH")

	BytesZAdd     = []byte("ZADD")
	BytesHSet     = []byte("HSET")
	Bytes         = []byte("SET")
	BytesExpireAt = []byte("PEXPIRE")
)

// RDBCallback is the callback interface defined to call
type RDBCallback interface {
	SelectDB(dbnum uint64)
	AuxField(key, data []byte)
	ResizeDB(size, esize uint64)
	EndOfRDB()

	CmdSet(key, val []byte, expire uint64)
	// List Command
	CmdRPush(key, val []byte)
	// Set
	CmdSAdd(key, val []byte)

	// ZSet
	CmdZAdd(key []byte, score float64, val []byte)

	// Hash
	CmdHSet(key, field, value []byte)
	CmdHSetInt(key, field []byte, value int64)

	// Expire
	ExpireAt(key []byte, expiry uint64)

	GetConn() net.Conn
}

// NewProtocolCallbacker convert them as callback
func NewProtocolCallbacker(addr string) *ProtocolCallbacker {
	p := &ProtocolCallbacker{
		addr: addr,
	}

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		log.Error("fail to dial with remote")
	} else {
		p.conn = conn
		p.bw = bufio.NewWriter(conn)
	}

	return p
}

// ProtocolCallbacker will get the callback data and convert into RESP
// protocol data into downstream.
type ProtocolCallbacker struct {
	addr string
	conn net.Conn
	bw   *bufio.Writer
}

// SelectDB impl Callback
func (r *ProtocolCallbacker) SelectDB(dbnum uint64) {
	log.Infof("call [SELECT %d]", dbnum)
}

// GetConn returns connection for protocolcallbacker only
func (r *ProtocolCallbacker) GetConn() net.Conn {
	return r.conn
}

// AuxField impl Callback
func (r *ProtocolCallbacker) AuxField(key, data []byte) {
	log.Infof("get aux field %s = %s", strconv.Quote(string(key)), strconv.Quote(string(data)))
}

// ResizeDB impl Callback
func (r *ProtocolCallbacker) ResizeDB(size, esize uint64) {
	log.Infof("RESIZE DB as size %d from old size %d", size, esize)
}

// EndOfRDB impl Callback
func (r *ProtocolCallbacker) EndOfRDB() {
	log.Infof("EndOfRDB...")
	_ = r.bw.Flush()
}

// CmdSet impl Callback
func (r *ProtocolCallbacker) CmdSet(key, val []byte, expire uint64) {
	r.handleErr(writePlainCmd(r.bw, BytesSet, key, val))

	if expire > 0 {
		r.ExpireAt(key, expire)
	}
}

// CmdRPush impl Callback
// List Command
func (r *ProtocolCallbacker) CmdRPush(key, val []byte) {
	r.handleErr(writePlainCmd(r.bw, BytesRPush, key, val))
}

// CmdSAdd impl Callback
// Set
func (r *ProtocolCallbacker) CmdSAdd(key, val []byte) {
	r.handleErr(writePlainCmd(r.bw, BytesSAdd, key, val))
}

// CmdZAdd impl Callback
// ZSet
func (r *ProtocolCallbacker) CmdZAdd(key []byte, score float64, val []byte) {
	r.handleErr(writePlainCmd(r.bw, BytesZAdd, key, val))
}

// CmdHSet impl Callback
// Hash
func (r *ProtocolCallbacker) CmdHSet(key, field, value []byte) {
	r.handleErr(write4ArgsCmd(r.bw, BytesHSet, key, field, value))
}

// CmdHSetInt impl Callback
func (r *ProtocolCallbacker) CmdHSetInt(key, field []byte, value int64) {
	r.handleErr(write4ArgsCmd(r.bw, BytesHSet, key, field, []byte(fmt.Sprintf("%d", value))))
}

// ExpireAt impl Callback
// Expire
func (r *ProtocolCallbacker) ExpireAt(key []byte, expiry uint64) {
	if expiry == 0 {
		return
	}

	r.handleErr(writePlainCmd(r.bw, BytesExpireAt, key, []byte(fmt.Sprintf("%d", expiry)), 2))
}

func (r *ProtocolCallbacker) handleErr(err error) {
	if err == nil {
		return
	}

	_ = r.conn.Close()
	r.conn, err = net.Dial("tcp", r.addr)
	if err != nil {
		log.Errorf("fail to reconnect due %s", err)
	}
}

func write4ArgsCmd(w *bufio.Writer, cmd, key, field, val []byte) (err error) {
	_ = writeBulkCount(w, 4)
	_ = writeToBulk(w, cmd)
	_ = writeToBulk(w, key)
	_ = writeToBulk(w, field)
	err = writeToBulk(w, val)
	return
}

func writePlainCmd(w *bufio.Writer, cmd, key, val []byte, size ...int) (err error) {
	count := 3
	if len(size) == 1 {
		count = size[0]
	}
	_ = writeBulkCount(w, count)
	_ = writeToBulk(w, cmd)
	_ = writeToBulk(w, key)
	err = writeToBulk(w, val)
	return
}

func writeBulkCount(w *bufio.Writer, size int) (err error) {
	_, err = w.WriteString(fmt.Sprintf("*%d\r\n", size))
	return
}

func writeToBulk(w *bufio.Writer, data []byte) (err error) {
	_, err = w.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(data), data))
	return
}
