package memcache

import (
	"bytes"
	errs "errors"
	"math"

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

// RequestType is the protocol-agnostic identifier for the command
type RequestType byte

func (rt RequestType) String() string {
	switch rt {
	case RequestTypeSet:
		return "set"
	case RequestTypeAdd:
		return "add"
	case RequestTypeReplace:
		return "replace"
	case RequestTypeAppend:
		return "append"
	case RequestTypePrepend:
		return "prepend"
	case RequestTypeCas:
		return "cas"
	case RequestTypeGet:
		return "get"
	case RequestTypeGets:
		return "gets"
	case RequestTypeDelete:
		return "delete"
	case RequestTypeIncr:
		return "incr"
	case RequestTypeDecr:
		return "decr"
	case RequestTypeTouch:
		return "touch"
	case RequestTypeGat:
		return "gat"
	case RequestTypeGats:
		return "gats"
	}
	return "unknown"
}

// all memcache request type
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
	ErrClosed         = errs.New("SERVER_ERROR connection closed")
	ErrPingerPong     = errs.New("SERVER_ERROR Pinger pong unexpected")
	ErrAssertRequest  = errs.New("SERVER_ERROR assert MC request not ok")
	ErrAssertResponse = errs.New("SERVER_ERROR assert MC response not ok")
)

// MCRequest is the mc client request type and data.
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
	rTp   RequestType
	key   []byte
	data  []byte
	batch bool
}

// Key get request key.
func (r *MCRequest) Key() []byte {
	return r.key
}

// IsBatch returns whether or not batch.
func (r *MCRequest) IsBatch() bool {
	return r.batch
}

// Batch returns sub MC request by multi key.
func (r *MCRequest) Batch() ([]proto.Request, *proto.Response) {
	n := bytes.Count(r.key, spaceBytes) // NOTE: like 'a_11 a_22 a_33'
	if n == 0 {
		return nil, nil
	}
	subs := make([]proto.Request, n+1)
	begin := 0
	end := bytes.IndexByte(r.key, spaceByte)
	for i := 0; i <= n; i++ {
		subs[i].Type = proto.CacheTypeMemcache
		subs[i].WithProto(&MCRequest{
			rTp:  r.rTp,
			key:  r.key[begin:end],
			data: r.data,
		})
		begin = end + 1
		if i >= n-1 { // NOTE: the last sub.
			end = len(r.key)
		} else {
			end = begin + bytes.IndexByte(r.key[end+1:], spaceByte)
		}
	}
	resp := &proto.Response{Type: proto.CacheTypeMemcache}
	resp.WithProto(&MCResponse{rTp: r.rTp})
	return subs, resp
}

func (r *MCRequest) String() string {
	return "type:" + r.rTp.String() + " key:" + string(r.key) + " data:" + string(r.data)
}

// MCResponse is the mc server response type and data.
type MCResponse struct {
	rTp  RequestType
	data []byte
}

// Merge merges subs response into self.
// NOTE: This normally means that the Merge func for an get|gets|gat|gats command.
func (r *MCResponse) Merge(subs []proto.Request) {
	if r.rTp != RequestTypeGet && r.rTp != RequestTypeGets && r.rTp != RequestTypeGat && r.rTp != RequestTypeGats {
		// TODO(felix): log or ???
		return
	}
	const endBytesLen = 5 // NOTE: endBytes length
	subl := len(subs)
	rebs := make([][]byte, subl)
	reln := 0
	for i := 0; i < subl; i++ {
		if err := subs[i].Resp.Err(); err != nil {
			// TODO(felix): log or ???
			continue
		}
		mcr, ok := subs[i].Resp.Proto().(*MCResponse)
		if !ok {
			// TODO(felix): log or ???
			continue
		}
		if bytes.Equal(mcr.data, endBytes) {
			continue
		}
		rebs[i] = mcr.data[:len(mcr.data)-endBytesLen]
		reln += len(rebs[i])
	}
	r.data = make([]byte, reln+endBytesLen) // TODO(felix): optimize
	off := 0
	for i := 0; i < subl; i++ {
		bs := rebs[i]
		if len(bs) == 0 {
			continue
		}
		copy(r.data[off:], bs)
		off += len(bs)
	}
	copy(r.data[off:], endBytes)
}
