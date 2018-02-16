package memcache

import (
	"github.com/pkg/errors"
)

// RequestType is the protocol-agnostic identifier for the command
type RequestType byte

// all memcache request type
const (
	RequestUnknown RequestType = iota
	RequestGet
	RequestGat
	RequestGetE
	RequestSet
	RequestAdd
	RequestReplace
	RequestAppend
	RequestPrepend
	RequestDelete
	RequestTouch
	RequestNoop
	RequestQuit
	RequestVersion
)

// errors
var (
	// error from client
	ErrBadRequest = errors.New("CLIENT_ERROR bad request")
	ErrBadFlags   = errors.New("CLIENT_ERROR flags is not a valid integer")
	ErrBadExptime = errors.New("CLIENT_ERROR exptime is not a valid integer")
	ErrBadLength  = errors.New("CLIENT_ERROR length is not a valid integer")

	// error from internal
	ErrInternal = errors.New("ERROR Internal error")
)

// Request is the client request type and data.
type Request struct {
	Type RequestType

	Key   []byte // NOTE: maybe multi key
	Value []byte
}
