package redis

import "strconv"

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

// resp is a redis resp protocol item.
type resp struct {
	rtype respType
	// in Bulk this is the size field
	// in array this is the count field
	data  []byte
	array []*resp
}

func newRespBulk(data []byte) *resp {
	return &resp{
		rtype: respBulk,
		data:  data,
		array: nil,
	}
}

func newRespPlain(rtype respType, data []byte) *resp {
	return &resp{
		rtype: rtype,
		data:  data,
		array: nil,
	}
}

func newRespString(val string) *resp {
	return &resp{
		rtype: respString,
		data:  []byte(val),
		array: nil,
	}
}

func newRespNull(rtype respType) *resp {
	return &resp{
		rtype: rtype,
		data:  nil,
		array: nil,
	}
}

func newRespArray(resps []*resp) *resp {
	return &resp{
		rtype: respArray,
		data:  nil,
		array: resps,
	}
}

func newRespArrayWithCapcity(length int) *resp {
	return &resp{
		rtype: respArray,
		data:  nil,
		array: make([]*resp, length),
	}
}

func newRespInt(val int) *resp {
	s := strconv.Itoa(val)
	return &resp{
		rtype: respInt,
		data:  []byte(s),
		array: nil,
	}
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
	if r.rtype == respString || r.rtype == respBulk {
		return string(r.data)
	}
	// TODO(wayslog): 实现其他的命令的 string
	return ""
}
