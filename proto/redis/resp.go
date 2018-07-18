package redis

import (
	"bytes"
	"strconv"
	"strings"
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

// resp is a redis server protocol item.
type resp struct {
	rtype respType
	// in Bulk this is the size field
	// in array this is the count field
	data  []byte
	array []*resp
}

func newRESPInt(val int) *resp {
	s := strconv.Itoa(val)
	return newRESPPlain(respInt, []byte(s))
}

func newRESPBulk(data []byte) *resp {
	return newRESPPlain(respBulk, data)
}

func newRESPPlain(rtype respType, data []byte) *resp {
	robj := respPool.Get().(*resp)
	robj.rtype = rtype
	robj.data = data
	robj.array = nil
	return robj
}

func newRESPString(val []byte) *resp {
	return newRESPPlain(respString, val)
}

func newRESPNull(rtype respType) *resp {
	return newRESPPlain(rtype, nil)
}

func newRESPArray(resps []*resp) *resp {
	robj := respPool.Get().(*resp)
	robj.rtype = respArray
	robj.data = []byte(strconv.Itoa(len(resps)))
	robj.array = resps
	return robj
}

func newRESPArrayWithCapcity(length int) *resp {
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

// String was only for debug
func (r *resp) String() string {
	if r.rtype == respArray {
		var sb strings.Builder
		sb.Write([]byte("["))
		for _, sub := range r.array[:len(r.array)-1] {
			sb.WriteString(sub.String())
			sb.WriteString(", ")
		}
		sb.WriteString(r.array[len(r.array)-1].String())
		sb.Write([]byte("]"))
		sb.WriteString("\n")
		return sb.String()
	}

	return strconv.Quote(string(r.data))
}

func (r *resp) bytes() []byte {
	var data = r.data
	var pos int
	if r.rtype == respBulk {
		pos = bytes.Index(data, crlfBytes) + 2
	}
	return data[pos:]
}
