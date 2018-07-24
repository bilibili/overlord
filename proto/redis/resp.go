package redis

import (
	"bytes"
	"overlord/lib/bufio"
	"overlord/proto"
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
	okBytes       = []byte("OK")
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
	// in order to reuse array.use arrayn to mark current obj.
	arrayn int
}

func (r *resp) reset() {
	r.rtype = 0
	r.data = r.data[:0]
	r.arrayn = 0
	for _, ar := range r.array {
		ar.reset()
	}
}

func newRESPInt(val int) *resp {
	s := strconv.Itoa(val)
	return newRESPPlain(respInt, []byte(s))
}

func (r *resp) isZero() bool {
	return r.rtype == 0
}

func (r *resp) setInt(val int) {
	s := strconv.Itoa(val)
	r.setPlain(respInt, []byte(s))
}

func newRESPBulk(data []byte) *resp {
	return newRESPPlain(respBulk, data)
}

func (r *resp) setBulk(data []byte) {
	r.setPlain(respBulk, data)
}

func newRESPPlain(rtype respType, data []byte) *resp {
	robj := respPool.Get().(*resp)
	robj.rtype = rtype
	robj.data = data
	robj.array = nil
	return robj
}

func (r *resp) setPlain(rtype respType, data []byte) {
	r.rtype = rtype
	r.data = data
	r.arrayn = 0
}

func newRESPString(val []byte) *resp {
	return newRESPPlain(respString, val)
}

func (r *resp) setString(val []byte) {
	r.setPlain(respString, val)
}

func newRESPNull(rtype respType) *resp {
	return newRESPPlain(rtype, nil)
}

func (r *resp) setNull(rtype respType) {
	r.setPlain(rtype, nil)
}

func newRESPArray(resps []*resp) *resp {
	robj := respPool.Get().(*resp)
	robj.rtype = respArray
	robj.data = []byte(strconv.Itoa(len(resps)))
	robj.array = resps
	robj.arrayn = len(resps)
	return robj
}

func (r *resp) setArray(resps []*resp) {
	r.rtype = respArray
	r.data = []byte(strconv.Itoa(len(resps)))
	r.array = resps
	r.arrayn = len(resps)
}

func (r *resp) nth(pos int) *resp {
	return r.array[pos]
}

func (r *resp) next() *resp {
	if r.arrayn < len(r.array) {
		robj := r.array[r.arrayn]
		r.arrayn++
		return robj
	} else {
		robj := respPool.Get().(*resp)
		r.array = append(r.array, robj)
		r.arrayn++
		return robj
	}
}

func (r *resp) isNull() bool {
	if r.rtype == respArray {
		return r.arrayn == 0
	}
	if r.rtype == respBulk {
		return r.data == nil
	}
	return false
}

func (r *resp) replace(pos int, newer *resp) {
	if pos < len(r.array) {
		r.array[pos] = newer
		r.arrayn = pos
	} else {
		r.array = append(r.array, newer)
		r.arrayn = len(r.array)
	}
}

func (r *resp) slice() []*resp {
	return r.array[:r.arrayn]
}

// Len represent the respArray type's length
func (r *resp) Len() int {
	return r.arrayn
}

// String was only for debug
func (r *resp) String() string {
	if r.rtype == respArray {
		var sb strings.Builder
		sb.Write([]byte("["))
		for _, sub := range r.array[:r.arrayn-1] {
			sb.WriteString(sub.String())
			sb.WriteString(", ")
		}
		sb.WriteString(r.array[r.arrayn-1].String())
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

func (r *resp) encode(w *bufio.Writer) error {
	switch r.rtype {
	case respInt:
		return r.encodeInt(w)
	case respError:
		return r.encodeError(w)
	case respString:
		return r.encodeString(w)
	case respBulk:
		return r.encodeBulk(w)
	case respArray:
		return r.encodeArray(w)
	}
	return nil
}

func (r *resp) encodeError(w *bufio.Writer) (err error) {
	return r.encodePlain(respErrorBytes, w)
}

func (r *resp) encodeInt(w *bufio.Writer) (err error) {
	return r.encodePlain(respIntBytes, w)

}

func (r *resp) encodeString(w *bufio.Writer) (err error) {
	return r.encodePlain(respStringBytes, w)
}

func (r *resp) encodePlain(rtypeBytes []byte, w *bufio.Writer) (err error) {
	err = w.Write(rtypeBytes)
	if err != nil {
		return
	}
	err = w.Write(r.data)
	if err != nil {
		return
	}
	err = w.Write(crlfBytes)
	return
}

func (r *resp) encodeBulk(w *bufio.Writer) (err error) {
	// NOTICE: we need not to convert robj.Len() as int
	// due number has been writen into data
	err = w.Write(respBulkBytes)
	if err != nil {
		return
	}
	if r.isNull() {
		err = w.Write(respNullBytes)
		return
	}

	err = w.Write(r.data)
	if err != nil {
		return
	}
	err = w.Write(crlfBytes)
	return
}

func (r *resp) encodeArray(w *bufio.Writer) (err error) {
	err = w.Write(respArrayBytes)
	if err != nil {
		return
	}

	if r.isNull() {
		err = w.Write(respNullBytes)
		return
	}
	// output size
	err = w.Write(r.data)
	if err != nil {
		return
	}
	err = w.Write(crlfBytes)

	for _, item := range r.slice() {
		item.encode(w)
		if err != nil {
			return
		}
	}
	return
}

func (r *resp) decode(msg *proto.Message) (err error) {
	if isComplex(r.nth(0).data) {
		err = newSubCmd(msg, r)
		if err != nil {
			return
		}
	} else {
		withReq(msg, r)
	}
	return
}

func withReq(m *proto.Message, robj *resp) {
	req := m.NextReq()
	if req == nil {
		m.WithRequest(newRequest(robj))
	} else {
		reqCmd := req.(*Request)
		reqCmd.setRESP(robj)
	}
}

// MergeType is used to decript the merge operation.
type MergeType = uint8

// merge types
const (
	MergeTypeCount MergeType = iota
	MergeTypeOk
	MergeTypeJoin
	MergeTypeBasic
)

func getMergeType(cmd []byte) MergeType {
	// TODO: impl with tire tree to search quickly
	if bytes.Equal(cmd, cmdMGetBytes) || bytes.Equal(cmd, cmdGetBytes) {
		return MergeTypeJoin
	}

	if bytes.Equal(cmd, cmdMSetBytes) {
		return MergeTypeOk
	}

	if bytes.Equal(cmd, cmdExistsBytes) || bytes.Equal(cmd, cmdDelBytes) {
		return MergeTypeCount
	}

	return MergeTypeBasic
}
