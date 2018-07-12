package binary

import (
	"bytes"
	"fmt"
	"overlord/lib/bufio"
	"overlord/lib/conv"
	libnet "overlord/lib/net"
	"overlord/proto"

	"github.com/pkg/errors"
)

// memcached binary protocol: https://github.com/memcached/memcached/wiki/BinaryProtocolRevamped
const (
	requestHeaderLen = 24
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
		msgs[i].Type = proto.CacheTypeMemcacheBinary
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
	head, err := p.br.ReadExact(requestHeaderLen)
	if err == bufio.ErrBufferFull {
		return
	} else if err != nil {
		err = errors.Wrap(err, "MC decoder while reading text line")
		return
	}
	fmt.Printf("wocaoa(%v)\n", head[0])
	req := p.request(m)
	parseHeader(head, req)
	if err != nil {
		err = errors.Wrap(err, "MC decoder while parse header")
		return
	}
	switch req.rTp {
	case RequestTypeSet, RequestTypeAdd, RequestTypeReplace,
		RequestTypeDelete, RequestTypeIncr, RequestTypeDecr, RequestTypeAppend, RequestTypePrepend, RequestTypeTouch, RequestTypeGat:
		return p.decodeCommon(m, req)
	// Retrieval commands:
	case RequestTypeGet, RequestTypeGetK, RequestTypeGetQ, RequestTypeGetKQ:
		return p.decodeRetrieval(m, req)
	}
	fmt.Printf("unsupport command(%x)\n", req.rTp)
	err = errors.Wrap(ErrBadRequest, "MC decoder unsupport command")
	return
}

func (p *proxyConn) decodeCommon(m *proto.Message, req *MCRequest) (err error) {
	bl, err := conv.Btoi(req.bodyLen)
	if err != nil {
		err = errors.Wrap(ErrBadLength, "MC decodeCommon body length")
		return
	}
	body, err := p.br.ReadExact(int(bl))
	if err != nil {
		err = errors.Wrap(err, "MC decodeCommon read exact body")
		return
	}
	el, err := conv.Btoi(req.extraLen)
	if err != nil {
		err = errors.Wrap(err, "MC decodeCommon conv extra length")
		return
	}
	kl, err := conv.Btoi(req.keyLen)
	if err != nil {
		err = errors.Wrap(err, "MC decodeCommon conv key length")
		return
	}
	req.key = body[el : el+kl]
	req.data = body
	return
}

func (p *proxyConn) decodeRetrieval(m *proto.Message, req *MCRequest) (err error) {
	for req.rTp == RequestTypeGetQ || req.rTp == RequestTypeGetKQ {
		var kl int64
		if kl, err = conv.Btoi(req.keyLen); err != nil {
			err = errors.Wrap(err, "MC decodeRetrieval conv keyq length")
			return
		}
		if req.key, err = p.br.ReadExact(int(kl)); err != nil {
			err = errors.Wrap(err, "MC decodeRetrieval read exact keyq")
			return
		}
		req.data = req.key
		req = p.request(m)
	}
	if req.rTp == RequestTypeGet || req.rTp == RequestTypeGetK {
		var kl int64
		if kl, err = conv.Btoi(req.keyLen); err != nil {
			err = errors.Wrap(err, "MC decodeRetrieval conv key length")
			return
		}
		if req.key, err = p.br.ReadExact(int(kl)); err != nil {
			err = errors.Wrap(err, "MC decodeRetrieval read exact key")
			return
		}
		req.data = req.key
		return
	}
	// no idea... this is a problem though.
	// unexpected patterns shouldn't come over the wire, so maybe it will
	// be OK to simply discount this situation. Probably not.
	err = errors.Wrap(ErrBadRequest, "MC decodeRetrieval unsupport command")
	return
}

func (p *proxyConn) request(m *proto.Message) *MCRequest {
	req := m.NextReq()
	if req == nil {
		req = GetReq()
		m.WithRequest(req)
	}
	return req.(*MCRequest)
}

func parseHeader(bs []byte, req *MCRequest) {
	req.magic = bs[0]
	req.rTp = RequestType(bs[1])
	req.keyLen = bs[2:4]
	req.extraLen = bs[4:5]
	req.bodyLen = bs[8:12]
	req.opaque = bs[12:16]
	req.cas = bs[16:24]
}

// Encode encode response and write into writer.
func (p *proxyConn) Encode(m *proto.Message) (err error) {
	_ = p.bw.Write(magicReqBytes) // NOTE: magic
	mcr, ok := m.Request().(*MCRequest)
	if !ok {
		err = errors.Wrap(ErrAssertReq, "MC Encoder assert request")
		return
	}
	_ = p.bw.Write(mcr.rTp.Bytes())
	_ = p.bw.Write(mcr.keyLen)
	_ = p.bw.Write(mcr.extraLen)
	_ = p.bw.Write(zeroBytes)
	if err = m.Err(); err != nil {
		_ = p.bw.Write(resopnseStatusInternalErrBytes)
	} else {
		_ = p.bw.Write(zeroTwoBytes)
	}
	_ = p.bw.Write(mcr.bodyLen)
	_ = p.bw.Write(mcr.opaque)
	_ = p.bw.Write(mcr.cas)
	if err == nil && !bytes.Equal(mcr.bodyLen, zeroTwoBytes) {
		_ = p.bw.Write(mcr.data)
	}
	if err = p.bw.Flush(); err != nil {
		err = errors.Wrap(err, "MC Encoder encode response flush bytes")
	}
	return
}
