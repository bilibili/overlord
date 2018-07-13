package redis

import (
	"strconv"
	"sync"
)

// respType is the type of redis resp
type respType = byte

// resp type define
const (
	respString respType = '+'
	respError  respType = '-'
	respInt    respType = ':'
	respBulk   respType = '$'
	respArray  respType = '*'
)

var (
	respStringBytes = []byte("+")
	respErrorBytes  = []byte("-")
	respIntBytes    = []byte(":")
	respBulkBytes   = []byte("$")
	respArrayBytes  = []byte("*")

	respNullBytes = []byte("-1\r\n")

	okBytes = []byte("OK")
)

var (
	respPool = sync.Pool{
		New: func() interface{} {
			return &resp{}
		},
	}
)

// resp is a redis resp protocol item.
type resp struct {
	rtype respType
	// in Bulk this is the size field
	// in array this is the count field
	data  []byte
	array []*resp
}

func newRespInt(val int) *resp {
	s := strconv.Itoa(val)
	return newRespPlain(respInt, []byte(s))
}

func newRespBulk(data []byte) *resp {
	return newRespPlain(respBulk, data)
}

func newRespPlain(rtype respType, data []byte) *resp {
	robj := respPool.Get().(*resp)
	robj.rtype = rtype
	robj.data = data
	robj.array = nil
	return robj
}

func newRespString(val []byte) *resp {
	return newRespPlain(respString, val)
}

func newRespNull(rtype respType) *resp {
	return newRespPlain(rtype, nil)
}

func newRespArray(resps []*resp) *resp {
	robj := respPool.Get().(*resp)
	robj.rtype = respArray
	robj.data = []byte(strconv.Itoa(len(resps)))
	robj.array = resps
	return robj
}

func newRespArrayWithCapcity(length int) *resp {
	robj := respPool.Get().(*resp)
	robj.rtype = respArray
	robj.data = nil
	robj.array = make([]*resp, length)
	return robj
}

func (r *resp) nth(pos int) *resp {
	return r.array[pos]
}

func (r *resp) isNull() bool {
	if r.rtype == respArray {
		return r.array == nil
	}
	if r.rtype == respBulk {
		return r.data == nil
	}
	return false
}

func (r *resp) replaceAll(begin int, newers []*resp) {
	copy(r.array[begin:], newers)
}

func (r *resp) replace(pos int, newer *resp) {
	r.array[pos] = newer
}

func (r *resp) slice() []*resp {
	return r.array
}

// Len represent the respArray type's length
func (r *resp) Len() int {
	return len(r.array)
}

func (r *resp) String() string {
	if r.rtype == respArray {
		return ""
	}

	return string(r.data)
}
