package redis

import (
	"bytes"
	"overlord/lib/bufio"
	"overlord/lib/conv"
)

const maxCloned = 16

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

	nullDataBytes = []byte("-1")
)

// RESP is resp export type.
type RESP = resp

// Type return resp type.
func (r *RESP) Type() byte {
	return r.rTp
}

// Data return resp data.
func (r *RESP) Data() []byte {
	return r.data
}

// Array return resp array.
func (r *RESP) Array() []*RESP {
	return r.array[:r.arrayn]
}

// Decode decode by Reader.
func (r *RESP) Decode(br *bufio.Reader) (err error) {
	return r.decode(br)
}

// Encode encode into Writer.
func (r *RESP) Encode(w *bufio.Writer) (err error) {
	return r.encode(w)
}

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

func (r *resp) Clone() *resp {
	nr := &resp{rTp: r.rTp}
	// bulk
	if r.rTp == respBulk {
		idx := bytes.Index(r.data, crlfBytes)
		if idx == -1 {
			nr.data = make([]byte, len(r.data))
			copy(nr.data, r.data)
			return nr
		}
		nr.data = make([]byte, len(r.data)-idx-1)
		copy(nr.data, r.data[idx+1:])
		return nr
	}

	// array
	if r.rTp == respArray {
		nr.arrayn = r.arrayn
		// TODO: copy ellipsized for avoid copy mulity
		// if nr.arrayn >= maxCloned {
		// 	nr.array = make([]*resp, maxCloned)
		// }

		nr.array = make([]*resp, r.arrayn)
		for i := 0; i < r.arrayn; i++ {
			sub := r.array[i]
			if sub != nil {
				nr.array[i] = sub.Clone()
			} else {
				nr.array[i] = &resp{rTp: respUnknown, data: []byte{}}
			}
		}
		return nr
	}

	// Plain
	nr.data = make([]byte, len(r.data))
	copy(nr.data, r.data)
	return nr
}

func (r *resp) reset() {
	r.rTp = respUnknown
	r.data = nil
	r.arrayn = 0
}

func (r *resp) copy(re *resp) {
	r.reset()
	r.rTp = re.rTp
	r.data = re.data
	for i := 0; i < re.arrayn; i++ {
		nre := r.next()
		nre.copy(re.array[i])
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
		_ = w.Write(respIntBytes)
	case respError:
		_ = w.Write(respErrorBytes)
	case respString:
		_ = w.Write(respStringBytes)
	}
	if len(r.data) > 0 {
		_ = w.Write(r.data)
	}
	err = w.Write(crlfBytes)
	return
}

func (r *resp) encodeBulk(w *bufio.Writer) (err error) {
	_ = w.Write(respBulkBytes)
	if len(r.data) > 0 {
		_ = w.Write(r.data)
	} else {
		_ = w.Write(nullDataBytes)
	}
	err = w.Write(crlfBytes)
	return
}

func (r *resp) encodeArray(w *bufio.Writer) (err error) {
	_ = w.Write(respArrayBytes)
	if len(r.data) > 0 {
		_ = w.Write(r.data)
	} else {
		_ = w.Write(nullDataBytes)
	}
	_ = w.Write(crlfBytes)
	for i := 0; i < r.arrayn; i++ {
		if err = r.array[i].encode(w); err != nil {
			return
		}
	}
	return
}
