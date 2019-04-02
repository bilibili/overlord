package memcache

import (
	errs "errors"
	"fmt"
	"overlord/pkg/types"
	"overlord/proxy/proto"
	"sync"
)

const (
	spaceByte = ' '
)

var (
	spaceBytes = []byte{' '}
	zeroBytes  = []byte{'0'}
	oneBytes   = []byte{'1'}
	crlfBytes  = []byte("\r\n")
	endBytes   = []byte("END\r\n")
	errorBytes = []byte("ERROR\r\n")

	setBytes     = []byte("set")
	addBytes     = []byte("add")
	replaceBytes = []byte("replace")
	appendBytes  = []byte("append")
	prependBytes = []byte("prepend")
	casBytes     = []byte("cas")
	getBytes     = []byte("get")
	getsBytes    = []byte("gets")
	deleteBytes  = []byte("delete")
	incrBytes    = []byte("incr")
	decrBytes    = []byte("decr")
	touchBytes   = []byte("touch")
	gatBytes     = []byte("gat")
	gatsBytes    = []byte("gats")
	unknownBytes = []byte("unknown")
	// storedBytes = []byte("STORED\r\n")
	// notStoredBytes = []byte("NOT_STORED\r\n")
	// existsBytes    = []byte("EXISTS\r\n")
	// notFoundBytes  = []byte("NOT_FOUND\r\n")
	// deletedBytes   = []byte("DELETED\r\n")
	// touchedBytes   = []byte("TOUCHED\r\n")
)

const (
	setString     = "set"
	addString     = "add"
	replaceString = "replace"
	appendString  = "append"
	prependString = "prepend"
	casString     = "cas"
	getString     = "get"
	getsString    = "gets"
	deleteString  = "delete"
	incrString    = "incr"
	decrString    = "decr"
	touchString   = "touch"
	gatString     = "gat"
	gatsString    = "gats"
	unknownString = "unknown"
)

// RequestType is the protocol-agnostic identifier for the command
type RequestType byte

func (rt RequestType) String() string {
	switch rt {
	case RequestTypeSet:
		return setString
	case RequestTypeAdd:
		return addString
	case RequestTypeReplace:
		return replaceString
	case RequestTypeAppend:
		return appendString
	case RequestTypePrepend:
		return prependString
	case RequestTypeCas:
		return casString
	case RequestTypeGet:
		return getString
	case RequestTypeGets:
		return getsString
	case RequestTypeDelete:
		return deleteString
	case RequestTypeIncr:
		return incrString
	case RequestTypeDecr:
		return decrString
	case RequestTypeTouch:
		return touchString
	case RequestTypeGat:
		return gatString
	case RequestTypeGats:
		return gatsString
	}
	return unknownString
}

// Bytes get reqtype bytes.
func (rt RequestType) Bytes() []byte {
	switch rt {
	case RequestTypeSet:
		return setBytes
	case RequestTypeAdd:
		return addBytes
	case RequestTypeReplace:
		return replaceBytes
	case RequestTypeAppend:
		return appendBytes
	case RequestTypePrepend:
		return prependBytes
	case RequestTypeCas:
		return casBytes
	case RequestTypeGet:
		return getBytes
	case RequestTypeGets:
		return getsBytes
	case RequestTypeDelete:
		return deleteBytes
	case RequestTypeIncr:
		return incrBytes
	case RequestTypeDecr:
		return decrBytes
	case RequestTypeTouch:
		return touchBytes
	case RequestTypeGat:
		return gatBytes
	case RequestTypeGats:
		return gatsBytes
	}
	return unknownBytes
}

// all memcache Msg type
const (
	RequestTypeUnknown RequestType = iota
	RequestTypeSet
	RequestTypeAdd
	RequestTypeReplace
	RequestTypeAppend
	RequestTypePrepend
	RequestTypeCas
	RequestTypeGet
	RequestTypeGets
	RequestTypeDelete
	RequestTypeIncr
	RequestTypeDecr
	RequestTypeTouch
	RequestTypeGat
	RequestTypeGats
)

var (
	withValueTypes = map[RequestType]struct{}{
		RequestTypeGet:  struct{}{},
		RequestTypeGets: struct{}{},
		RequestTypeGat:  struct{}{},
		RequestTypeGats: struct{}{},
	}
)

// errors
var (
	// ERROR means the client sent a nonexistent command name.
	ErrError = errs.New("ERROR")

	// CLIENT_ERROR
	// means some sort of client error in the input line, i.e. the input
	// doesn't conform to the protocol in some way. <error> is a
	// human-readable error string.
	ErrBadRequest = errs.New("CLIENT_ERROR bad request")
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
	ErrClosed      = errs.New("SERVER_ERROR connection closed")
	ErrPingerPong  = errs.New("SERVER_ERROR Pinger pong unexpected")
	ErrAssertReq   = errs.New("SERVER_ERROR assert request not ok")
	ErrBadResponse = errs.New("SERVER_ERROR bad response")
)

// MCRequest is the mc client Msg type and data.
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
type MCRequest struct {
	rTp  RequestType
	key  []byte
	data []byte
}

var msgPool = &sync.Pool{
	New: func() interface{} {
		return NewReq()
	},
}

// GetReq get the msg from pool
func GetReq() *MCRequest {
	return msgPool.Get().(*MCRequest)
}

// NewReq return new mc req.
func NewReq() *MCRequest {
	return &MCRequest{}
}

// Put put req back to pool.
func (r *MCRequest) Put() {
	r.rTp = RequestTypeUnknown
	r.key = r.key[:0]
	r.data = r.data[:0]
	msgPool.Put(r)
}

// CmdString will return string of cmd for prom report
func (r *MCRequest) CmdString() string {
	return r.rTp.String()
}

// Cmd get Msg cmd.
func (r *MCRequest) Cmd() []byte {
	return r.rTp.Bytes()
}

// Key get Msg key.
func (r *MCRequest) Key() []byte {
	return r.key
}

func (r *MCRequest) String() string {
	return fmt.Sprintf("type:%s key:%s data:%s", r.rTp.Bytes(), r.key, r.data)
}

// Slowlog record the slowlog entry
func (r *MCRequest) Slowlog() (slog *proto.SlowlogEntry) {
	slog = proto.NewSlowlogEntry(types.CacheTypeMemcache)
	if r.rTp == RequestTypeGat || r.rTp == RequestTypeGats {
		slog.Cmd = [][]byte{r.rTp.Bytes(), r.data, r.key, crlfBytes}
	} else {
		slog.Cmd = [][]byte{r.rTp.Bytes(), r.key, r.data}
	}
	return slog
}
