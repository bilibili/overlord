package memcache

import (
	"bytes"

	"overlord/lib/bufio"
	"overlord/lib/conv"
	libnet "overlord/lib/net"
	"overlord/proto"

	"github.com/pkg/errors"
)

// memcached protocol: https://github.com/memcached/memcached/blob/master/doc/protocol.txt
const (
	errorPrefix       = "ERROR"
	clientErrorPrefix = "CLIENT_ERROR "
	serverErrorPrefix = "SERVER_ERROR "
)

var (
	serverErrorBytes = []byte(serverErrorPrefix)
)

type proxyConn struct {
	br        *bufio.Reader
	bw        *bufio.Writer
	completed bool
}

// NewProxyConn new a memcache decoder and encode.
func NewProxyConn(rw *libnet.Conn) proto.ProxyConn {
	p := &proxyConn{
		// TODO: optimus zero
		br:        bufio.NewReader(rw, bufio.Get(1024)),
		bw:        bufio.NewWriter(rw),
		completed: true,
	}
	return p
}

func (p *proxyConn) Decode(msgs []*proto.Message) ([]*proto.Message, error) {
	var err error
	// if completed, means that we have parsed all the buffered
	// if not completed, we need only to parse the buffered message
	if p.completed {
		err = p.br.Read()
		if err != nil {
			return nil, err
		}
	}

	for i := range msgs {
		p.completed = false
		// set msg type
		msgs[i].Type = proto.CacheTypeMemcache
		// decode
		err = p.decode(msgs[i])
		if err == bufio.ErrBufferFull {
			p.completed = true
			msgs[i].Reset()
			return msgs[:i], nil
		} else if err != nil {
			msgs[i].Reset()
			return msgs[:i], err
		}
		msgs[i].MarkStart()
	}
	return msgs, nil
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
	conv.UpdateToLower(line[bg:ed])
	switch string(line[bg:ed]) {
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
		p.br.Advance(-((keyE - keyB) + 1 + len(mtype.Bytes())))
		return
	} else if err != nil {
		err = errors.Wrap(err, "MC decoder while read data by length")
		return
	}
	if !bytes.HasSuffix(data, crlfBytes) {
		err = errors.Wrap(ErrBadRequest, "MC decoder data not end with CRLF")
		return
	}
	p.withReq(m, mtype, key, data)
	return
}

func (p *proxyConn) decodeRetrieval(m *proto.Message, bs []byte, reqType RequestType) (err error) {
	var (
		b, e int
		ns   = bs[:]
	)
	for {
		ns = ns[e:]
		b, e = nextField(ns)
		if !legalKey(ns[b:e]) {
			err = errors.Wrap(ErrBadKey, "MC Decoder retrieval Msg legal key")
			return
		}

		if b == len(ns)-2 {
			break
		}

		p.withReq(m, reqType, ns[b:e], crlfBytes)
	}
	return
}

func (p *proxyConn) decodeDelete(m *proto.Message, bs []byte, reqType RequestType) (err error) {
	keyB, keyE := nextField(bs)
	key := bs[keyB:keyE]
	if !legalKey(key) {
		err = errors.Wrap(ErrBadKey, "MC decoder delete request legal key")
		return
	}
	p.withReq(m, reqType, key, crlfBytes)
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
	p.withReq(m, reqType, key, ns)
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
	p.withReq(m, reqType, key, ns)
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
	)
	for {
		ns = ns[e:]
		b, e = nextField(ns)
		if !legalKey(ns[b:e]) {
			err = errors.Wrap(ErrBadKey, "MC Decoder retrieval Msg legal key")
			return
		}
		p.withReq(m, reqType, ns[b:e], expBs)
		if e == len(ns)-2 {
			break
		}
	}
	return
}

func (p *proxyConn) withReq(m *proto.Message, rtype RequestType, key []byte, data []byte) {
	req := m.NextReq()
	if req == nil {
		req := GetReq()
		req.rTp = rtype
		req.key = key
		req.data = data
		m.WithRequest(req)
	} else {
		mcreq := req.(*MCRequest)
		mcreq.rTp = rtype
		mcreq.key = key
		mcreq.data = data
	}
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
	ns := bytes.TrimSpace(bs[:pos])
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
		_ = p.bw.Write(serverErrorBytes)
		_ = p.bw.Write([]byte(se))
		_ = p.bw.Write(crlfBytes)
	} else {
		var bs []byte
		reqs := m.Requests()
		for _, req := range reqs {
			mcr, ok := req.(*MCRequest)
			if !ok {
				_ = p.bw.Write(serverErrorBytes)
				_ = p.bw.Write([]byte(ErrAssertReq.Error()))
				_ = p.bw.Write(crlfBytes)
			} else {
				_, ok := withValueTypes[mcr.rTp]
				if ok && m.IsBatch() {
					bs = bytes.TrimSuffix(mcr.data, endBytes)
				} else {
					bs = mcr.data
				}
				if len(bs) == 0 {
					continue
				}
				_ = p.bw.Write(bs)
			}
		}
		if m.IsBatch() {
			_ = p.bw.Write(endBytes)
		}
	}
	return
}

func (p *proxyConn) Flush() (err error) {
	if err = p.bw.Flush(); err != nil {
		err = errors.Wrap(err, "MC Encoder encode response flush bytes")
	}
	return
}
