package memcache

import (
	"bytes"
	"fmt"
	"io"
	"strings"

	"github.com/felixhao/overlord/lib/bufio"
	"github.com/felixhao/overlord/lib/conv"
	"github.com/felixhao/overlord/proto"
	"github.com/pkg/errors"
)

// memcached protocol: https://github.com/memcached/memcached/blob/master/doc/protocol.txt
const (
	errorPrefix       = "ERROR"
	clientErrorPrefix = "CLIENT_ERROR "
	serverErrorPrefix = "SERVER_ERROR "

	encoderBufferSize = 64 * 1024 // NOTE: keep writing data into client, so relatively large
)

type proxyConn struct {
	br *bufio.Reader
	bw *bufio.Writer
}

// NewProxyConn new a memcache decoder and encode.
func NewProxyConn(rw io.ReadWriter) proto.ProxyConn {
	p := &proxyConn{
		br: bufio.NewReader(rw, nil),
		bw: bufio.NewWriter(rw),
	}
	return p
}

func (p *proxyConn) Decode() (m *proto.Message, err error) {
	println("entry decode")
	// new message
	m = proto.NewMessage()
	m.Type = proto.CacheTypeMemcache
	// bufio reset buffer
	p.br.ResetBuffer(m.Buffer())
	line, err := p.br.ReadUntil(delim)
	if err != nil {
		err = errors.Wrapf(err, "MC decoder while reading text line")
		return
	}
	bg, ed := nextField(line)
	lower := conv.ToLower(line[bg:ed])
	if len(lower) == 0 {
		err = errors.Wrapf(ErrBadRequest, "MC request get bad with zero length")
		return
	}
	fmt.Println("cmd:", string(lower), bg, ed, line[bg:ed], line)
	switch string(lower) {
	// Storage commands:
	case "set":
		return m, p.decodeStorage(m, line[ed:], RequestTypeSet, false)
	case "add":
		return m, p.decodeStorage(m, line[ed:], RequestTypeAdd, false)
	case "replace":
		return m, p.decodeStorage(m, line[ed:], RequestTypeReplace, false)
	case "append":
		return m, p.decodeStorage(m, line[ed:], RequestTypeAppend, false)
	case "prepend":
		return m, p.decodeStorage(m, line[ed:], RequestTypePrepend, false)
	case "cas":
		return m, p.decodeStorage(m, line[ed:], RequestTypeCas, true)
	// Retrieval commands:
	case "get":
		return m, p.decodeRetrieval(m, line[ed:], RequestTypeGet)
	case "gets":
		return m, p.decodeRetrieval(m, line[ed:], RequestTypeGets)
	// Deletion
	case "delete":
		return m, p.decodeDelete(m, line[ed:], RequestTypeDelete)
	// Increment/Decrement:
	case "incr":
		return m, p.decodeIncrDecr(m, line[ed:], RequestTypeIncr)
	case "decr":
		return m, p.decodeIncrDecr(m, line[ed:], RequestTypeDecr)
	// Touch:
	case "touch":
		return m, p.decodeTouch(m, line[ed:], RequestTypeTouch)
	// Get And Touch:
	case "gat":
		return m, p.decodeGetAndTouch(m, line[ed:], RequestTypeGat)
	case "gats":
		return m, p.decodeGetAndTouch(m, line[ed:], RequestTypeGats)
	}
	err = errors.Wrapf(ErrBadRequest, "MC decoder unsupport command")
	return
}

// Encode encode response and write into writer.
func (p *proxyConn) Encode(m *proto.Message) (err error) {
	if err = m.Err(); err != nil {
		se := errors.Cause(err).Error()
		if !strings.HasPrefix(se, errorPrefix) && !strings.HasPrefix(se, clientErrorPrefix) && !strings.HasPrefix(se, serverErrorPrefix) { // NOTE: the mc error protocol
			p.bw.WriteString(serverErrorPrefix)
		}
		p.bw.WriteString(se)
		p.bw.Write(crlfBytes)
	} else {
		res := m.Response()
		for _, bs := range res {
			p.bw.Write(bs)
		}
	}
	if fe := p.bw.Flush(); fe != nil {
		err = errors.Wrap(fe, "MC Encoder encode response flush bytes")
	}
	return
}

func (p *proxyConn) decodeStorage(m *proto.Message, bs []byte, mtype RequestType, cas bool) (err error) {
	keyB, keyE := nextField(bs)
	key := bs[keyB:keyE]
	if !legalKey(key) {
		err = errors.Wrap(ErrBadKey, "MC decoder delete request legal key")
		return
	}
	println("key:", string(key))
	// length
	length, err := findLength(bs, cas)
	if err != nil {
		println("err:", err.Error())
		err = errors.Wrap(err, "MC decoder while parse length")
		return
	}
	println("length:", length)
	keyOffset := len(bs) - keyE
	p.br.Advance(-keyOffset)
	fmt.Println("need read", keyOffset+length+2)
	data, err := p.br.ReadFull(keyOffset + length + 2)
	if err != nil {
		err = errors.Wrap(err, "MC decoder while read data by length")
		return
	}
	println("data:", string(data))
	if !bytes.HasSuffix(data, crlfBytes) {
		err = errors.Wrap(ErrBadLength, "MC decoder data not end with CRLF")
		return
	}
	m.WithRequest(&MCRequest{
		rTp:  mtype,
		key:  key,
		data: data,
	})
	return
}

func (p *proxyConn) decodeRetrieval(m *proto.Message, bs []byte, reqType RequestType) (err error) {
	var (
		b, e int
		ns   = bs[:]
		rs   = make([]proto.Request, 0, bytes.Count(bs, spaceBytes))
	)
	for {
		ns = ns[e:]
		b, e = nextField(ns)
		if !legalKey(ns[b:e]) {
			err = errors.Wrap(ErrBadKey, "MC Decoder retrieval Msg legal key")
			return
		}
		req := &MCRequest{
			rTp:  reqType,
			key:  ns[b:e],
			data: crlfBytes,
		}
		println("key:", string(req.key))
		rs = append(rs, req)
		if e == len(ns)-2 {
			break
		}
	}
	m.WithRequest(rs...)
	return
}

func (p *proxyConn) decodeDelete(m *proto.Message, bs []byte, reqType RequestType) (err error) {
	keyB, keyE := nextField(bs)
	key := bs[keyB:keyE]
	if !legalKey(key) {
		err = errors.Wrap(ErrBadKey, "MC decoder delete request legal key")
		return
	}
	println("key:", string(key))
	m.WithRequest(&MCRequest{
		rTp:  reqType,
		key:  key,
		data: crlfBytes,
	})
	return
}

func (p *proxyConn) decodeIncrDecr(m *proto.Message, bs []byte, reqType RequestType) (err error) {
	keyB, keyE := nextField(bs)
	key := bs[keyB:keyE]
	if !legalKey(key) {
		err = errors.Wrap(ErrBadKey, "MC decoder delete request legal key")
		return
	}
	println("key:", string(key))
	ns := bs[keyE:]
	vB, vE := nextField(ns)
	valueBs := ns[vB:vE]
	println("incrdect:", string(valueBs))
	if !bytes.Equal(valueBs, oneBytes) {
		if _, err = conv.Btoi(valueBs); err != nil {
			err = errors.Wrapf(ErrBadRequest, "MC decoder incrDecr request parse value(%s)", valueBs)
			return
		}
	}
	m.WithRequest(&MCRequest{
		rTp:  reqType,
		key:  key,
		data: ns,
	})
	return
}

func (p *proxyConn) decodeTouch(m *proto.Message, bs []byte, reqType RequestType) (err error) {
	keyB, keyE := nextField(bs)
	key := bs[keyB:keyE]
	if !legalKey(key) {
		err = errors.Wrap(ErrBadKey, "MC decoder delete request legal key")
		return
	}
	println("key:", string(key))
	ns := bs[keyE:]
	eB, eE := nextField(ns)
	expBs := ns[eB:eE]
	println("exp:", string(expBs))
	if !bytes.Equal(expBs, zeroBytes) {
		if _, err = conv.Btoi(expBs); err != nil {
			err = errors.Wrapf(ErrBadRequest, "MC decoder touch request parse exptime(%s)", expBs)
			return
		}
	}
	m.WithRequest(&MCRequest{
		rTp:  reqType,
		key:  key,
		data: ns,
	})
	return
}

func (p *proxyConn) decodeGetAndTouch(m *proto.Message, bs []byte, reqType RequestType) (err error) {
	eB, eE := nextField(bs)
	expBs := bs[eB:eE]
	if !bytes.Equal(expBs, zeroBytes) {
		if _, err = conv.Btoi(expBs); err != nil {
			err = errors.Wrapf(ErrBadRequest, "MC decoder touch request parse exptime(%s)", expBs)
			return
		}
	}
	println("exp:", string(expBs))
	var (
		b, e int
		ns   = bs[eE:]
		rs   = make([]proto.Request, 0, bytes.Count(ns, spaceBytes))
	)
	for {
		ns = ns[e:]
		b, e = nextField(ns)
		if !legalKey(ns[b:e]) {
			err = errors.Wrap(ErrBadKey, "MC Decoder retrieval Msg legal key")
			return
		}
		req := &MCRequest{
			rTp:  reqType,
			key:  ns[b:e],
			data: expBs, // NOTE: no contains '\r\n'!!!
		}
		println("key:", string(req.key))
		rs = append(rs, req)
		if e == len(ns)-2 {
			break
		}
	}
	m.WithRequest(rs...)
	return
}

func nextField(bs []byte) (begin, end int) {
	begin = noSpaceIdx(bs)
	offset := bytes.IndexByte(bs[begin:], spaceByte)
	if offset == -1 {
		if bytes.HasSuffix(bs[begin:], crlfBytes) {
			end = len(bs) - 2
		} else {
			end = len(bs)
		}
		return
	}
	end = begin + offset
	return
}

func findLength(bs []byte, cas bool) (int, error) {

	pos := len(bs) - 2
	if cas {
		// skip cas filed
		high := revNoSpacIdx(bs[:pos])
		low := revSpacIdx(bs[:high])
		pos = low
	}
	if pos < 0 {
		fmt.Println("find bytes:", bs, "find str:", string(bs))
		return 0, ErrBadLength
	}

	up := revNoSpacIdx(bs[:pos]) + 1
	low := revSpacIdx(bs[:up]) + 1
	lengthBs := bs[low:up]
	println("lenbs:", string(lengthBs))
	length, err := conv.Btoi(lengthBs)
	if err != nil {
		return -1, ErrBadLength
	}
	return int(length), nil
}

// Currently the length limit of a key is set at 250 characters.
// the key must not include control characters or whitespace.
func legalKey(key []byte) bool {
	if len(key) > 250 {
		return false
	}
	for i := 0; i < len(key); i++ {
		if key[i] < ' ' {
			return false
		}
		if key[i] <= ' ' {
			return false
		}
		if key[i] == 0x7f {
			return false
		}
	}
	return true
}

func noSpaceIdx(bs []byte) int {
	for i := 0; i < len(bs); i++ {
		if bs[i] != spaceByte {
			return i
		}
	}
	return len(bs)
}

func revNoSpacIdx(bs []byte) int {
	for i := len(bs) - 1; i > -1; i-- {
		if bs[i] != spaceByte {
			return i
		}
	}
	return -1
}

func revSpacIdx(bs []byte) int {
	for i := len(bs) - 1; i > -1; i-- {
		if bs[i] == spaceByte {
			return i
		}
	}
	return -1
}
