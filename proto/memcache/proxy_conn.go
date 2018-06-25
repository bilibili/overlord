package memcache

import (
	"bytes"
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
)

type proxyConn struct {
	br *bufio.Reader
	bw *bufio.Writer
}

// NewProxyConn new a memcache decoder and encode.
func NewProxyConn(rw io.ReadWriter) proto.ProxyConn {
	p := &proxyConn{
		// TODO: optimus zero
		br: bufio.NewReader(rw, bufio.Get(1024)),
		bw: bufio.NewWriter(rw),
	}
	return p
}

func (p *proxyConn) Read() error {
	return p.br.Read()
}

func (p *proxyConn) Decode(msg *proto.Message) (completed bool, err error) {
	// set msg type
	msg.Type = proto.CacheTypeMemcache
	// decode
	err = p.decode(msg)
	if err == bufio.ErrBufferFull {
		completed = false
		err = nil
	} else {
		completed = true
	}
	return
}

func (p *proxyConn) decode(m *proto.Message) (err error) {
	// bufio reset buffer
	line, err := p.br.ReadSlice(delim)
	if err == bufio.ErrBufferFull {
		return
	} else if err != nil {
		err = errors.Wrapf(err, "MC decoder while reading text line")
		return
	}
	bg, ed := nextField(line)
	lower := conv.ToLower(line[bg:ed])
	switch string(lower) {
	// Storage commands:
	case "set":
		return p.decodeStorage(m, line[ed:], RequestTypeSet, false)
	case "add":
		return p.decodeStorage(m, line[ed:], RequestTypeAdd, false)
	case "replace":
		return p.decodeStorage(m, line[ed:], RequestTypeReplace, false)
	case "append":
		return p.decodeStorage(m, line[ed:], RequestTypeAppend, false)
	case "prepend":
		return p.decodeStorage(m, line[ed:], RequestTypePrepend, false)
	case "cas":
		return p.decodeStorage(m, line[ed:], RequestTypeCas, true)
	// Retrieval commands:
	case "get":
		return p.decodeRetrieval(m, line[ed:], RequestTypeGet)
	case "gets":
		return p.decodeRetrieval(m, line[ed:], RequestTypeGets)
	// Deletion
	case "delete":
		return p.decodeDelete(m, line[ed:], RequestTypeDelete)
	// Increment/Decrement:
	case "incr":
		return p.decodeIncrDecr(m, line[ed:], RequestTypeIncr)
	case "decr":
		return p.decodeIncrDecr(m, line[ed:], RequestTypeDecr)
	// Touch:
	case "touch":
		return p.decodeTouch(m, line[ed:], RequestTypeTouch)
	// Get And Touch:
	case "gat":
		return p.decodeGetAndTouch(m, line[ed:], RequestTypeGat)
	case "gats":
		return p.decodeGetAndTouch(m, line[ed:], RequestTypeGats)
	}
	err = errors.Wrapf(ErrBadRequest, "MC decoder unsupport command")
	return
}

func (p *proxyConn) decodeStorage(m *proto.Message, bs []byte, mtype RequestType, cas bool) (err error) {
	keyB, keyE := nextField(bs)
	key := bs[keyB:keyE]
	if !legalKey(key) {
		err = errors.Wrap(ErrBadKey, "MC decoder storage request legal key")
		return
	}
	// length
	length, err := findLength(bs, cas)
	if err != nil {
		err = errors.Wrap(err, "MC decoder while parse length")
		return
	}
	keyOffset := len(bs) - keyE
	p.br.Advance(-keyOffset) // NOTE: data contains "<flags> <exptime> <bytes> <cas unique> [noreply]\r\n"
	data, err := p.br.ReadExact(keyOffset + length + 2)
	if err == bufio.ErrBufferFull {
		return
	} else if err != nil {
		err = errors.Wrap(err, "MC decoder while read data by length")
		return
	}
	if !bytes.HasSuffix(data, crlfBytes) {
		err = errors.Wrap(ErrBadRequest, "MC decoder data not end with CRLF")
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
		err = errors.Wrap(ErrBadKey, "MC decoder incr/decr request legal key")
		return
	}
	ns := bs[keyE:]
	vB, vE := nextField(ns)
	valueBs := ns[vB:vE]
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
		err = errors.Wrap(ErrBadKey, "MC decoder touch request legal key")
		return
	}
	ns := bs[keyE:]
	eB, eE := nextField(ns)
	expBs := ns[eB:eE]
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
	pos := len(bs) - 2 // NOTE: trim right "\r\n"
	ns := bs[:pos]
	if cas {
		// skip cas filed
		si := revSpacIdx(ns)
		if si == -1 {
			return -1, ErrBadLength
		}
		ns = ns[:si]
	}
	low := revSpacIdx(ns) + 1
	if low == 0 {
		return -1, ErrBadLength
	}
	length, err := conv.Btoi(ns[low:])
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

func revSpacIdx(bs []byte) int {
	for i := len(bs) - 1; i > -1; i-- {
		if bs[i] == spaceByte {
			return i
		}
	}
	return -1
}

// Encode encode response and write into writer.
func (p *proxyConn) Encode(m *proto.Message) (err error) {
	if err = m.Err(); err != nil {
		se := errors.Cause(err).Error()
		if !strings.HasPrefix(se, errorPrefix) && !strings.HasPrefix(se, clientErrorPrefix) && !strings.HasPrefix(se, serverErrorPrefix) { // NOTE: the mc error protocol
			_ = p.bw.WriteString(serverErrorPrefix)
		}
		_ = p.bw.WriteString(se)
		_ = p.bw.Write(crlfBytes)
	} else {
		mcr, ok := m.Request().(*MCRequest)
		if !ok {
			_ = p.bw.WriteString(serverErrorPrefix)
			_ = p.bw.WriteString(ErrAssertMsg.Error())
			_ = p.bw.Write(crlfBytes)
		} else {
			res := m.Response()
			_, ok := withValueTypes[mcr.rTp]
			trimEnd := ok && m.IsBatch()
			for _, bs := range res {
				if trimEnd {
					bs = bytes.TrimSuffix(bs, endBytes)
				}
				if len(bs) == 0 {
					continue
				}
				_ = p.bw.Write(bs)
			}
			if trimEnd {
				_ = p.bw.Write(endBytes)
			}
		}
	}
	if err = p.bw.Flush(); err != nil {
		err = errors.Wrap(err, "MC Encoder encode response flush bytes")
	}
	return
}
