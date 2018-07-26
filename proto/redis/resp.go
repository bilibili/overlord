package redis

import (
	"overlord/lib/bufio"
	"overlord/lib/conv"
)

// respType is the type of redis resp
type respType = byte

// resp type define
const (
	respUnknown respType = '0'
	respString  respType = '+'
	respError   respType = '-'
	respInt     respType = ':'
	respBulk    respType = '$'
	respArray   respType = '*'
)

var (
	respStringBytes = []byte("+")
	respErrorBytes  = []byte("-")
	respIntBytes    = []byte(":")
	respBulkBytes   = []byte("$")
	respArrayBytes  = []byte("*")
)

// resp is a redis server protocol item.
type resp struct {
	rTp respType
	// in Bulk this is the size field
	// in array this is the count field
	data  []byte
	array []*resp
	// in order to reuse array.use arrayn to mark current obj.
	arrayn int
}

func (r *resp) reset() {
	r.rTp = respUnknown
	r.data = nil
	r.arrayn = 0
	for _, ar := range r.array {
		ar.reset()
	}
}

func (r *resp) next() *resp {
	if r.arrayn < len(r.array) {
		nr := r.array[r.arrayn]
		nr.reset()
		r.arrayn++
		return nr
	}
	nr := &resp{}
	nr.reset()
	r.array = append(r.array, nr)
	r.arrayn++
	return nr
}

func (r *resp) decode(br *bufio.Reader) (err error) {
	r.reset()
	// start read
	line, err := br.ReadLine()
	if err != nil {
		return err
	}
	rTp := line[0]
	r.rTp = rTp
	switch rTp {
	case respString, respInt, respError:
		r.data = line[1 : len(line)-2]
	case respBulk:
		err = r.decodeBulk(line, br)
	case respArray:
		err = r.decodeArray(line, br)
	default:
		err = ErrBadRequest
	}
	return
}

func (r *resp) decodeBulk(line []byte, br *bufio.Reader) (err error) {
	ls := len(line)
	sBs := line[1 : ls-2]
	size, err := conv.Btoi(sBs)
	if err != nil {
		return
	}
	if size == -1 {
		r.data = nil
		return
	}
	br.Advance(-(ls - 1))
	all := ls - 1 + int(size) + 2
	data, err := br.ReadExact(all)
	if err == bufio.ErrBufferFull {
		br.Advance(-1)
		return err
	} else if err != nil {
		return
	}
	r.data = data[:len(data)-2]
	return
}

func (r *resp) decodeArray(line []byte, br *bufio.Reader) (err error) {
	ls := len(line)
	sBs := line[1 : ls-2]
	size, err := conv.Btoi(sBs)
	if err != nil {
		return
	}
	if size == -1 {
		r.data = nil
		return
	}
	r.data = sBs
	mark := br.Mark()
	for i := 0; i < int(size); i++ {
		nre := r.next()
		if err = nre.decode(br); err != nil {
			br.AdvanceTo(mark)
			br.Advance(-ls)
			return
		}
	}
	return
}

func (r *resp) encode(w *bufio.Writer) (err error) {
	switch r.rTp {
	case respInt, respString, respError:
		err = r.encodePlain(w)
	case respBulk:
		err = r.encodeBulk(w)
	case respArray:
		err = r.encodeArray(w)
	}
	return
}

func (r *resp) encodePlain(w *bufio.Writer) (err error) {
	switch r.rTp {
	case respInt:
		err = w.Write(respIntBytes)
	case respError:
		err = w.Write(respErrorBytes)
	case respString:
		err = w.Write(respStringBytes)
	}
	if err != nil {
		return
	}
	if len(r.data) > 0 {
		if err = w.Write(r.data); err != nil {
			return
		}
	}
	err = w.Write(crlfBytes)
	return
}

func (r *resp) encodeBulk(w *bufio.Writer) (err error) {
	if err = w.Write(respBulkBytes); err != nil {
		return
	}
	if len(r.data) > 0 {
		err = w.Write(r.data)
	} else {
		err = w.Write(respNullBytes)
		return
	}
	if err != nil {
		return
	}
	err = w.Write(crlfBytes)
	return
}

func (r *resp) encodeArray(w *bufio.Writer) (err error) {
	if err = w.Write(respArrayBytes); err != nil {
		return
	}
	if len(r.data) > 0 {
		err = w.Write(r.data)
	} else {
		err = w.Write(respNullBytes)
	}
	if err != nil {
		return
	}
	if err = w.Write(crlfBytes); err != nil {
		return
	}
	for i := 0; i < r.arrayn; i++ {
		if err = r.array[i].encode(w); err != nil {
			return
		}
	}
	return
}
