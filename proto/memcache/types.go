package memcache

import (
	"bytes"
	errs "errors"
	"math"
	"sync"

	"github.com/felixhao/overlord/lib/bufio"
	"github.com/felixhao/overlord/proto"
)

const (
	delim     = '\n'
	spaceByte = ' '
	maxUint32 = math.MaxUint32
)

var (
	spaceBytes     = []byte{' '}
	zeroBytes      = []byte{'0'}
	oneBytes       = []byte{'1'}
	crlfBytes      = []byte("\r\n")
	endBytes       = []byte("END\r\n")
	storedBytes    = []byte("STORED\r\n")
	notStoredBytes = []byte("NOT_STORED\r\n")
	existsBytes    = []byte("EXISTS\r\n")
	notFoundBytes  = []byte("NOT_FOUND\r\n")
	deletedBytes   = []byte("DELETED\r\n")
	touchedBytes   = []byte("TOUCHED\r\n")
)

var (
	bufPool = &sync.Pool{New: func() interface{} {
		return &bufio.SliceAlloc{}
	}}
)

// MsgType is the protocol-agnostic identifier for the command
type MsgType byte

func (rt MsgType) String() string {
	switch rt {
	case MsgTypeSet:
		return "set"
	case MsgTypeAdd:
		return "add"
	case MsgTypeReplace:
		return "replace"
	case MsgTypeAppend:
		return "append"
	case MsgTypePrepend:
		return "prepend"
	case MsgTypeCas:
		return "cas"
	case MsgTypeGet:
		return "get"
	case MsgTypeGets:
		return "gets"
	case MsgTypeDelete:
		return "delete"
	case MsgTypeIncr:
		return "incr"
	case MsgTypeDecr:
		return "decr"
	case MsgTypeTouch:
		return "touch"
	case MsgTypeGat:
		return "gat"
	case MsgTypeGats:
		return "gats"
	}
	return "unknown"
}

// all memcache Msg type
const (
	MsgTypeUnknown MsgType = iota
	MsgTypeSet
	MsgTypeAdd
	MsgTypeReplace
	MsgTypeAppend
	MsgTypePrepend
	MsgTypeCas
	MsgTypeGet
	MsgTypeGets
	MsgTypeDelete
	MsgTypeIncr
	MsgTypeDecr
	MsgTypeTouch
	MsgTypeGat
	MsgTypeGats
)

// errors
var (
	// ERROR means the client sent a nonexistent command name.
	ErrError = errs.New("ERROR")

	// CLIENT_ERROR
	// means some sort of client error in the input line, i.e. the input
	// doesn't conform to the protocol in some way. <error> is a
	// human-readable error string.
	ErrBadMsg     = errs.New("CLIENT_ERROR bad Msg")
	ErrBadKey     = errs.New("CLIENT_ERROR key invalid")
	ErrBadFlags   = errs.New("CLIENT_ERROR flags is not a valid integer")
	ErrBadExptime = errs.New("CLIENT_ERROR exptime is not a valid integer")
	ErrBadLength  = errs.New("CLIENT_ERROR length is not a valid integer")
	ErrBadCas     = errs.New("CLIENT_ERROR cas is not a valid integer")

	// SERVER_ERROR
	// means some sort of server error prevents the server from carrying
	// out the command. <error> is a human-readable error string. In cases
	// of severe server errors, which make it impossible to continue
	// serving the client (this shouldn't normally happen), the server will
	// close the connection after sending the error line. This is the only
	// case in which the server closes a connection to a client.
	ErrClosed         = errs.New("SERVER_ERROR connection closed")
	ErrPingerPong     = errs.New("SERVER_ERROR Pinger pong unexpected")
	ErrAssertMsg      = errs.New("SERVER_ERROR assert MC Msg not ok")
	ErrAssertResponse = errs.New("SERVER_ERROR assert MC response not ok")
	ErrBadResponse    = errs.New("SERVER_ERROR bad response")
)

// MCMsg is the mc client Msg type and data.
// Storage commands:
// 	<command name> <key> <flags> <exptime> <bytes> [noreply]\r\n
//  cas <key> <flags> <exptime> <bytes> <cas unique> [noreply]\r\n
// Retrieval commands:
//  get|gets <key>*\r\n
// Deletion command:
//  delete <key> [noreply]\r\n
// Increment/Decrement:
//  incr|decr <key> <value> [noreply]\r\n
// Touch:
// 	touch <key> <exptime> [noreply]\r\n
// Get And Touch:
// 	gat|gats <exptime> <key>*\r\n
type MCMsg struct {
	rTp   MsgType
	key   []byte
	data  []byte
	batch bool

	resp   [][]byte // FIXME:use global buffer pool.
	subReq []MCMsg
}

// Cmd get Msg cmd.
func (r *MCMsg) Cmd() string {
	return r.rTp.String()
}

// Key get Msg key.
func (r *MCMsg) Key() []byte {
	return r.key
}

// IsBatch returns whether or not batch.
func (r *MCMsg) IsBatch() bool {
	return r.batch
}

// Batch returns sub MC Msg by multi key.
func (r *MCMsg) Batch() []proto.Msg {
	n := bytes.Count(r.key, spaceBytes) // NOTE: like 'a_11 a_22 a_33'
	if n == 0 {
		return nil
	}
	subs := make([]proto.Msg, n+1)
	r.subReq = make([]MCMsg, n+1)
	begin := 0
	end := bytes.IndexByte(r.key, spaceByte)
	for i := 0; i <= n; i++ {
		subs[i].Type = proto.CacheTypeMemcache
		r.subReq[i] = MCMsg{
			rTp:  r.rTp,
			key:  r.key[begin:end],
			data: r.data,
		}
		subs[i].WithProto(&r.subReq[i])
		begin = end + 1
		if i >= n-1 { // NOTE: the last sub.
			end = len(r.key)
		} else {
			end = begin + bytes.IndexByte(r.key[end+1:], spaceByte)
		}
	}
	return subs
}

func (r *MCMsg) String() string {
	return "type:" + r.rTp.String() + " key:" + string(r.key) + " data:" + string(r.data)
}

// Merge merge subreq's response.
func (r *MCMsg) Merge() [][]byte {
	for _, sub := range r.subReq {
		r.resp = append(r.resp, sub.resp...)
	}
	return r.resp
}
