package memcache

import (
	"bytes"

	"overlord/pkg/bufio"
	"overlord/pkg/conv"
	libnet "overlord/pkg/net"
	"overlord/pkg/types"
	"overlord/proxy/proto"
	"overlord/version"

	"github.com/pkg/errors"
)

// memcached protocol: https://github.com/memcached/memcached/blob/master/doc/protocol.txt
const (
	serverErrorPrefix = "SERVER_ERROR "

	proxyReadBufSize = 1024
)

var (
	serverErrorBytes  = []byte(serverErrorPrefix)
	versionReplyBytes = []byte("VERSION ")
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
		br:        bufio.NewReader(rw, bufio.Get(proxyReadBufSize)),
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
		msgs[i].Type = types.CacheTypeMemcache
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
	line, err := p.br.ReadLine()
	if err == bufio.ErrBufferFull {
		return
	} else if err != nil {
		err = errors.WithStack(err)
		return
	}
	bg, ed := nextField(line)
	conv.UpdateToLower(line[bg:ed])
	switch string(line[bg:ed]) {
	// Storage commands:
	case setString:
		return p.decodeStorage(m, line[ed:], RequestTypeSet)
	case addString:
		return p.decodeStorage(m, line[ed:], RequestTypeAdd)
	case replaceString:
		return p.decodeStorage(m, line[ed:], RequestTypeReplace)
	case appendString:
		return p.decodeStorage(m, line[ed:], RequestTypeAppend)
	case prependString:
		return p.decodeStorage(m, line[ed:], RequestTypePrepend)
	case casString:
		return p.decodeStorage(m, line[ed:], RequestTypeCas)
	// Retrieval commands:
	case getString:
		return p.decodeRetrieval(m, line[ed:], RequestTypeGet)
	case getsString:
		return p.decodeRetrieval(m, line[ed:], RequestTypeGets)
	// Deletion
	case deleteString:
		return p.decodeDelete(m, line[ed:], RequestTypeDelete)
	// Increment/Decrement:
	case incrString:
		return p.decodeIncrDecr(m, line[ed:], RequestTypeIncr)
	case decrString:
		return p.decodeIncrDecr(m, line[ed:], RequestTypeDecr)
	// Touch:
	case touchString:
		return p.decodeTouch(m, line[ed:], RequestTypeTouch)
	// Get And Touch:
	case gatString:
		return p.decodeGetAndTouch(m, line[ed:], RequestTypeGat)
	case gatsString:
		return p.decodeGetAndTouch(m, line[ed:], RequestTypeGats)
	case quitString:
		return p.decodeQuit(m, line[ed:])
	case versionString:
		return p.decodeVersion(m, line[ed:])
	}
	err = errors.WithStack(ErrBadRequest)
	return
}

func (p *proxyConn) decodeVersion(m *proto.Message, key []byte) (err error) {
	WithReq(m, RequestTypeVersion, key, crlfBytes)
	return
}

func (p *proxyConn) decodeQuit(m *proto.Message, key []byte) (err error) {
	WithReq(m, RequestTypeQuit, key, crlfBytes)
	return
}

func (p *proxyConn) decodeStorage(m *proto.Message, bs []byte, mtype RequestType) (err error) {
	keyB, keyE := nextField(bs)
	key := bs[keyB:keyE]
	if !legalKey(key) {
		err = errors.WithStack(ErrBadKey)
		return
	}

	noreply := mtype == RequestTypeSet && bytes.Contains(bs[keyE:], noreplyBytes)
	if noreply {
		mtype = RequestTypeSetNoreply
	}

	// length
	length, err := parseLen(bs[keyE:], 3)
	if err != nil {
		err = errors.WithStack(err)
		return
	}

	keyOffset := len(bs) - keyE
	p.br.Advance(-keyOffset) // NOTE: data contains "<flags> <exptime> <bytes> <cas unique> [noreply]\r\n"
	data, err := p.br.ReadExact(keyOffset + length + 2)
	if err == bufio.ErrBufferFull {
		p.br.Advance(-((keyE - keyB) + 1 + len(mtype.Bytes())))
		return
	} else if err != nil {
		err = errors.WithStack(err)
		return
	}
	if !bytes.HasSuffix(data, crlfBytes) {
		err = errors.WithStack(ErrBadRequest)
		return
	}

	WithReq(m, mtype, key, data)
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
			err = errors.WithStack(ErrBadKey)
			return
		}
		if b == len(ns)-2 {
			break
		}
		WithReq(m, reqType, ns[b:e], crlfBytes)
	}
	return
}

func (p *proxyConn) decodeDelete(m *proto.Message, bs []byte, reqType RequestType) (err error) {
	keyB, keyE := nextField(bs)
	key := bs[keyB:keyE]
	if !legalKey(key) {
		err = errors.WithStack(ErrBadKey)
		return
	}
	WithReq(m, reqType, key, crlfBytes)
	return
}

func (p *proxyConn) decodeIncrDecr(m *proto.Message, bs []byte, reqType RequestType) (err error) {
	keyB, keyE := nextField(bs)
	key := bs[keyB:keyE]
	if !legalKey(key) {
		err = errors.WithStack(ErrBadKey)
		return
	}
	ns := bs[keyE:]
	vB, vE := nextField(ns)
	valueBs := ns[vB:vE]
	if !bytes.Equal(valueBs, oneBytes) {
		if _, err = conv.Btoi(valueBs); err != nil {
			err = errors.WithStack(ErrBadRequest)
			return
		}
	}
	WithReq(m, reqType, key, ns)
	return
}

func (p *proxyConn) decodeTouch(m *proto.Message, bs []byte, reqType RequestType) (err error) {
	keyB, keyE := nextField(bs)
	key := bs[keyB:keyE]
	if !legalKey(key) {
		err = errors.WithStack(ErrBadKey)
		return
	}
	ns := bs[keyE:]
	eB, eE := nextField(ns)
	expBs := ns[eB:eE]
	if !bytes.Equal(expBs, zeroBytes) {
		if _, err = conv.Btoi(expBs); err != nil {
			err = errors.WithStack(ErrBadRequest)
			return
		}
	}
	WithReq(m, reqType, key, ns)
	return
}

func (p *proxyConn) decodeGetAndTouch(m *proto.Message, bs []byte, reqType RequestType) (err error) {
	eB, eE := nextField(bs)
	expBs := bs[eB:eE]
	if !bytes.Equal(expBs, zeroBytes) {
		if _, err = conv.Btoi(expBs); err != nil {
			err = errors.WithStack(ErrBadRequest)
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
			err = errors.WithStack(ErrBadKey)
			return
		}
		WithReq(m, reqType, ns[b:e], expBs)
		if e == len(ns)-2 {
			break
		}
	}
	return
}

// WithReq will fill with memcache request.
func WithReq(m *proto.Message, rtype RequestType, key []byte, data []byte) {
	req := m.NextReq()
	if req == nil {
		req := GetReq()
		req.respType = rtype
		req.key = req.key[:0]
		req.key = append(req.key, key...)
		req.data = req.data[:0]
		req.data = append(req.data, data...)
		m.WithRequest(req)
	} else {
		mcreq := req.(*MCRequest)
		mcreq.respType = rtype
		mcreq.key = mcreq.key[:0]
		mcreq.key = append(mcreq.key, key...)
		mcreq.data = mcreq.data[:0]
		mcreq.data = append(mcreq.data, data...)
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

func parseLen(bs []byte, nth int) (int, error) {
	ns := nthFiled(bs, nth)
	ival, err := conv.Btoi(ns)
	if err != nil {
		return -1, ErrBadLength
	}
	return int(ival), err
}

func nthFiled(bs []byte, nth int) []byte {
	for i := 0; i < nth; i++ {
		begin, end := nextField(bs)
		if i == nth-1 {
			return bs[begin:end]
		}
		bs = bs[end:]
	}
	return bs
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
	if me := m.Err(); me != nil {
		se := errors.Cause(me).Error()
		_ = p.bw.Write(serverErrorBytes)
		_ = p.bw.Write([]byte(se))
		err = p.bw.Write(crlfBytes)
		return
	}

	if !m.IsBatch() {
		mcr, ok := m.Request().(*MCRequest)
		if !ok {
			_ = p.bw.Write(serverErrorBytes)
			_ = p.bw.Write([]byte(ErrAssertReq.Error()))
			err = p.bw.Write(crlfBytes)
			return
		}

		if mcr.respType == RequestTypeQuit {
			err = proto.ErrQuit
			return
		}
		if mcr.respType == RequestTypeVersion {
			_ = p.bw.Write(versionReplyBytes)
			_ = p.bw.Write(version.Bytes())
			err = p.bw.Write(crlfBytes)
			return
		}
		if mcr.respType == RequestTypeSetNoreply {
			return
		}

		err = p.bw.Write(mcr.data)
		return
	}

	for _, req := range m.Requests() {
		mcr, ok := req.(*MCRequest)
		if !ok {
			_ = p.bw.Write(serverErrorBytes)
			_ = p.bw.Write([]byte(ErrAssertReq.Error()))
			err = p.bw.Write(crlfBytes)
			return
		}
		var bs []byte
		if _, ok := withValueTypes[mcr.respType]; ok {
			bs = bytes.TrimSuffix(mcr.data, endBytes)
		} else {
			bs = mcr.data
		}
		if len(bs) == 0 {
			continue
		}
		_ = p.bw.Write(bs)
	}

	err = p.bw.Write(endBytes)
	return
}

func (p *proxyConn) Flush() (err error) {
	return p.bw.Flush()
}
