package binary

import (
	errs "errors"
	"fmt"
	"overlord/proxy/proto"
	"sync"
)

const (
	magicReq  = 0x80
	magicResp = 0x81
)

var (
	magicReqBytes  = []byte{magicReq}
	magicRespBytes = []byte{magicResp}
	zeroBytes      = []byte{0x00}
	zeroTwoBytes   = []byte{0x00, 0x00}
	zeroFourBytes  = []byte{0x00, 0x00, 0x00, 0x00}
	zeroEightBytes = []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}
)

// RequestType is the protocol-agnostic identifier for the command
type RequestType byte

// all memcache request type
const (
	RequestTypeGet     RequestType = 0x00
	RequestTypeSet     RequestType = 0x01
	RequestTypeAdd     RequestType = 0x02
	RequestTypeReplace RequestType = 0x03
	RequestTypeDelete  RequestType = 0x04
	RequestTypeIncr    RequestType = 0x05
	RequestTypeDecr    RequestType = 0x06
	RequestTypeGetQ    RequestType = 0x09
	RequestTypeNoop    RequestType = 0x0a
	RequestTypeGetK    RequestType = 0x0c
	RequestTypeGetKQ   RequestType = 0x0d
	RequestTypeAppend  RequestType = 0x0e
	RequestTypePrepend RequestType = 0x0f
	// RequestTypeSetQ     = 0x11
	// RequestTypeAddQ     = 0x12
	// RequestTypeReplaceQ = 0x13
	// RequestTypeIncrQ    = 0x15
	// RequestTypeDecrQ    = 0x16
	// RequestTypeAppendQ  = 0x19
	// RequestTypePrependQ = 0x1a
	RequestTypeTouch RequestType = 0x1c
	RequestTypeGat   RequestType = 0x1d
	// RequestTypeGatQ    = 0x1e
	RequestTypeUnknown RequestType = 0xff
)

var (
	getBytes     = []byte{byte(RequestTypeGet)}
	setBytes     = []byte{byte(RequestTypeSet)}
	addBytes     = []byte{byte(RequestTypeAdd)}
	replaceBytes = []byte{byte(RequestTypeReplace)}
	deleteBytes  = []byte{byte(RequestTypeDelete)}
	incrBytes    = []byte{byte(RequestTypeIncr)}
	decrBytes    = []byte{byte(RequestTypeDecr)}
	getQBytes    = []byte{byte(RequestTypeGetQ)}
	noopBytes    = []byte{byte(RequestTypeNoop)}
	getKBytes    = []byte{byte(RequestTypeGetK)}
	getKQBytes   = []byte{byte(RequestTypeGetKQ)}
	appendBytes  = []byte{byte(RequestTypeAppend)}
	prependBytes = []byte{byte(RequestTypePrepend)}
	// setQBytes     = []byte{byte(RequestTypeSetQ)}
	// addQBytes     = []byte{byte(RequestTypeAddQ)}
	// replaceQBytes = []byte{byte(RequestTypeReplaceQ)}
	// incrQBytes    = []byte{byte(RequestTypeIncrQ)}
	// decrQBytes    = []byte{byte(RequestTypeDecrQ)}
	// appendQBytes  = []byte{byte(RequestTypeAppendQ)}
	// prependQBytes = []byte{byte(RequestTypePrependQ)}
	touchBytes = []byte{byte(RequestTypeTouch)}
	gatBytes   = []byte{byte(RequestTypeGat)}
	// gatQBytes     = []byte{byte(RequestTypeGatQ)}
	unknownBytes = []byte{byte(RequestTypeUnknown)}
)

const (
	getString     = "get"
	setString     = "set"
	addString     = "add"
	replaceString = "replace"
	deleteString  = "delete"
	incrString    = "incr"
	decrString    = "decr"
	getQString    = "getq"
	noopString    = "noop"
	getKString    = "getk"
	getKQString   = "getkq"
	appendString  = "append"
	prependString = "prepend"
	// setQString     = "setq"
	// addQString     = "addq"
	// replaceQString = "replaceq"
	// incrQString    = "incrq"
	// decrQString    = "decrq"
	// appendQString  = "appendq"
	// prependQString = "prependq"
	touchString = "touch"
	gatString   = "gat"
	// gatQString     = "gatq"
	unknownString = "unknown"
)

// Bytes get reqtype bytes.
func (rt RequestType) Bytes() []byte {
	switch rt {
	case RequestTypeGet:
		return getBytes
	case RequestTypeSet:
		return setBytes
	case RequestTypeAdd:
		return addBytes
	case RequestTypeReplace:
		return replaceBytes
	case RequestTypeDelete:
		return deleteBytes
	case RequestTypeIncr:
		return incrBytes
	case RequestTypeDecr:
		return decrBytes
	case RequestTypeGetQ:
		return getQBytes
	case RequestTypeNoop:
		return noopBytes
	case RequestTypeGetK:
		return getKBytes
	case RequestTypeGetKQ:
		return getKQBytes
	case RequestTypeAppend:
		return appendBytes
	case RequestTypePrepend:
		return prependBytes
	// case RequestTypeSetQ:
	// 	return setQBytes
	// case RequestTypeAddQ:
	// 	return addQBytes
	// case RequestTypeReplaceQ:
	// 	return replaceQBytes
	// case RequestTypeIncrQ:
	// 	return incrQBytes
	// case RequestTypeDecrQ:
	// 	return decrQBytes
	// case RequestTypeAppendQ:
	// 	return appendQBytes
	// case RequestTypePrependQ:
	// 	return prependQBytes
	case RequestTypeTouch:
		return touchBytes
	case RequestTypeGat:
		return gatBytes
		// case RequestTypeGatQ:
		// 	return gatQBytes
	}
	return unknownBytes
}

// String get reqtype string.
func (rt RequestType) String() string {
	switch rt {
	case RequestTypeGet:
		return getString
	case RequestTypeSet:
		return setString
	case RequestTypeAdd:
		return addString
	case RequestTypeReplace:
		return replaceString
	case RequestTypeDelete:
		return deleteString
	case RequestTypeIncr:
		return incrString
	case RequestTypeDecr:
		return decrString
	case RequestTypeGetQ:
		return getQString
	case RequestTypeNoop:
		return noopString
	case RequestTypeGetK:
		return getKString
	case RequestTypeGetKQ:
		return getKQString
	case RequestTypeAppend:
		return appendString
	case RequestTypePrepend:
		return prependString
	// case RequestTypeSetQ:
	// 	return setQString
	// case RequestTypeAddQ:
	// 	return addQString
	// case RequestTypeReplaceQ:
	// 	return replaceQString
	// case RequestTypeIncrQ:
	// 	return incrQString
	// case RequestTypeDecrQ:
	// 	return decrQString
	// case RequestTypeAppendQ:
	// 	return appendQString
	// case RequestTypePrependQ:
	// 	return prependQString
	case RequestTypeTouch:
		return touchString
	case RequestTypeGat:
		return gatString
		// case RequestTypeGatQ:
		// 	return gatQString
	}
	return unknownString
}

// ResponseStatus is the protocol-agnostic identifier for the response status
type ResponseStatus byte

// all memcache response status
const (
	ResponseStatusNoErr         = 0x0000
	ResponseStatusKeyNotFound   = 0x0001
	ResponseStatusKeyExists     = 0x0002
	ResponseStatusValueTooLarge = 0x0003
	ResponseStatusInvalidArg    = 0x0004
	ResponseStatusItemNotStored = 0x0005
	ResponseStatusNonNumeric    = 0x0006
	ResponseStatusUnknownCmd    = 0x0081
	ResponseStatusOutOfMem      = 0x0082
	ResponseStatusNotSupported  = 0x0083
	ResponseStatusInternalErr   = 0x0084
	ResponseStatusBusy          = 0x0085
	ResponseStatusTemporary     = 0x0086
)

var (
	resopnseStatusInternalErrBytes = []byte{0x00, 0x84}
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
	ErrBadLength  = errs.New("CLIENT_ERROR length is not a valid integer")

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
type MCRequest struct {
	magic    byte // Already known, since we're here
	rTp      RequestType
	keyLen   []byte
	extraLen []byte
	// dataType []byte // Always 0
	// vBucket  []byte // Not used
	status  []byte // response status
	bodyLen []byte
	opaque  []byte
	cas     []byte

	key  []byte
	data []byte
}

var msgPool = &sync.Pool{
	New: func() interface{} {
		return newReq()
	},
}

// GetReq get the msg from pool
func GetReq() *MCRequest {
	return msgPool.Get().(*MCRequest)
}

// newReq return new mc req.
func newReq() *MCRequest {
	return &MCRequest{
		keyLen:   []byte{0x00, 0x00},
		extraLen: []byte{0x00},
		status:   []byte{0x00, 0x00},
		bodyLen:  []byte{0x00, 0x00, 0x00, 0x00},
		opaque:   []byte{0x00, 0x00, 0x00, 0x00},
		cas:      []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
	}
}

// Put put req back to pool.
func (r *MCRequest) Put() {
	r.rTp = RequestTypeUnknown
	r.key = r.key[:0]
	r.data = r.data[:0]
	msgPool.Put(r)
}

// CmdString get cmd.
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
	return fmt.Sprintf("type:%s key:%s data:%s", r.rTp.String(), r.key, r.data)
}

// Slowlog record the slowlog entry
func (r *MCRequest) Slowlog() *proto.SlowlogEntry {
	return nil
}
