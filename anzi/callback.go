package anzi

import (
	"bufio"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"strconv"
	"sync"
	"time"

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
	BytesExpireAt = []byte("EXPIREAT")
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
		addr:     addr,
		endOfRDB: make(chan struct{}, 1),
	}

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		log.Error("fail to dial with remote")
	} else {
		p.conn = conn
		p.bw = bufio.NewWriter(conn)
		p.br = bufio.NewReader(conn)
		go p.ignoreRecv()
	}

	return p
}

// ProtocolCallbacker will get the callback data and convert into RESP
// protocol data into downstream.
type ProtocolCallbacker struct {
	addr string

	lock sync.RWMutex
	conn net.Conn
	bw   *bufio.Writer
	br   *bufio.Reader

	endOfRDB chan struct{}
}

func (r *ProtocolCallbacker) ignoreRecv() {
	for {
		select {
		case <-r.endOfRDB:
			return
		default:
		}

		r.lock.RLock()
		size, err := io.Copy(ioutil.Discard, r.br)
		r.lock.RUnlock()
		if size == 0 {
			time.Sleep(time.Second)
		}
		if err != nil {
			log.Warnf("fail to discard reader due %s", err)
		}
	}
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
	r.endOfRDB <- struct{}{}
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

	r.handleErr(writePlainCmd(r.bw, BytesExpireAt, key, []byte(fmt.Sprintf("%d", expiry)), 3))
}

func (r *ProtocolCallbacker) handleErr(err error) {
	if err == nil {
		return
	}

	var conn net.Conn
	conn, err = net.Dial("tcp", r.addr)
	if err != nil {
		log.Errorf("fail to reconnect due %s", err)
		return
	}

	_ = r.conn.Close()
	r.lock.Lock()
	r.conn = conn
	r.bw = bufio.NewWriter(r.conn)
	r.br = bufio.NewReader(r.conn)
	r.lock.Unlock()
}

func write4ArgsCmd(w *bufio.Writer, cmd, key, field, val []byte) (err error) {
	_ = writeArrayCount(w, 4)
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
	_ = writeArrayCount(w, count)
	_ = writeToBulk(w, cmd)
	_ = writeToBulk(w, key)
	err = writeToBulk(w, val)
	return
}

func writeArrayCount(w *bufio.Writer, size int) (err error) {
	_, err = w.WriteString(fmt.Sprintf("*%d\r\n", size))
	return
}

func writeToBulk(w *bufio.Writer, data []byte) (err error) {
	_, err = w.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(data), data))
	return
}
