package redis

import (
	"errors"

	"strings"

	"github.com/felixhao/overlord/proto"
)

var (
	crlfBytes = []byte{'\r', '\n'}
)

// errors
var (
	ErrProxyFail        = errors.New("fail to send proxy")
	ErrRequestBadFormat = errors.New("redis must be a RESP array")
)

// respType is the type of redis resp
type respType = byte

// resp type define
const (
	respString = '+'
	respError  = '-'
	respInt    = ':'
	respBulk   = '$'
	respArray  = '*'
)

// resp is a redis resp protocol item.
type resp struct {
	rtype respType
	data  []byte
	array []*resp
}

func (r *resp) nth(pos int) *resp {
	return r.array[pos]
}

func (r *resp) sliceFrom(begin int) []*resp {
	return r.array[begin:]
}

func (r *resp) isNull() bool {
	return r.array == nil || len(r.array) == 0
}

func (r *resp) String() string {
	if r.rtype == respString || r.rtype == respBulk {
		return string(r.data)
	}
	// TODO(wayslog): 实现其他的命令的 string
	return ""
}

// RRequest is the type of a complete redis command
type RRequest struct {
	respObj *resp
}

// Cmd get the cmd
func (rr *RRequest) Cmd() string {
	return strings.ToUpper(rr.respObj.nth(0).String())
}

func (rr *RRequest) Key() []byte {
	return rr.respObj.nth(1).data
}

func (rr *RRequest) IsBatch() bool {
	return true
}

func (rr *RRequest) Batch() ([]proto.Request, *proto.Response) {
	return nil, nil
}
