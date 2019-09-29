package binary

import (
	"bytes"
	"encoding/binary"

	"overlord/pkg/bufio"
	libnet "overlord/pkg/net"
	"overlord/pkg/types"
	"overlord/proxy/proto"

	"github.com/pkg/errors"
)

// memcached binary protocol: https://github.com/memcached/memcached/wiki/BinaryProtocolRevamped
const (
	requestHeaderLen = 24
	proxyReadBufSize = 1024
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
		msgs[i].Type = types.CacheTypeMemcacheBinary
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
NEXTGET:
	// bufio reset buffer
	head, err := p.br.ReadExact(requestHeaderLen)
	if err == bufio.ErrBufferFull {
		return
	} else if err != nil {
		err = errors.WithStack(err)
		return
	}
	req := p.request(m)
	parseHeader(head, req, true)
	if err != nil {
		err = errors.WithStack(err)
		return
	}
	switch req.respType {
	case RequestTypeNoop, RequestTypeVersion, RequestTypeQuit, RequestTypeQuitQ:
		req.key = req.key[:0]
		req.data = req.data[:0]
		return
	case RequestTypeSet, RequestTypeAdd, RequestTypeReplace, RequestTypeGet, RequestTypeGetK,
		RequestTypeDelete, RequestTypeIncr, RequestTypeDecr, RequestTypeAppend, RequestTypePrepend,
		RequestTypeTouch, RequestTypeGat:
		if err = p.decodeCommon(m, req); err == bufio.ErrBufferFull {
			p.br.Advance(-requestHeaderLen)
			return
		}
		return
	case RequestTypeGetQ, RequestTypeGetKQ, RequestTypeSetQ, RequestTypeAddQ, RequestTypeReplaceQ,
		RequestTypeIncrQ, RequestTypeDecrQ, RequestTypeAppendQ, RequestTypePrependQ:
	REQAGAIN:
		if err = p.decodeCommon(m, req); err == bufio.ErrBufferFull {
			if err = p.br.Read(); err != nil {
				return
			}
			goto REQAGAIN // NOTE: try to read again for this request
		}
		goto NEXTGET
	}
	err = errors.Wrapf(ErrBadRequest, "MC decoder unsupport command:%d", req.respType)
	return
}

func (p *proxyConn) decodeCommon(m *proto.Message, req *MCRequest) (err error) {
	bl := binary.BigEndian.Uint32(req.bodyLen)
	body, err := p.br.ReadExact(int(bl))
	if err == bufio.ErrBufferFull {
		return
	} else if err != nil {
		err = errors.WithStack(err)
		return
	}
	el := uint8(req.extraLen[0])
	kl := binary.BigEndian.Uint16(req.keyLen)
	// copy
	req.key = req.key[:0]
	req.key = append(req.key, body[int(el):int(el)+int(kl)]...)
	req.data = req.data[:0]
	req.data = append(req.data, body...)
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

func parseHeader(bs []byte, req *MCRequest, isDecode bool) {
	req.magic = bs[0]
	if isDecode {
		req.respType = RequestType(bs[1])
	}
	copy(req.keyLen, bs[2:4])
	copy(req.extraLen, bs[4:5])
	if !isDecode {
		copy(req.status, bs[6:8])
	}
	copy(req.bodyLen, bs[8:12])
	copy(req.opaque, bs[12:16])
	copy(req.cas, bs[16:24])
}

// Encode encode response and write into writer.
func (p *proxyConn) Encode(m *proto.Message) (err error) {
	reqs := m.Requests()
	for _, req := range reqs {
		mcr, ok := req.(*MCRequest)
		if !ok {
			err = errors.WithStack(ErrAssertReq)
			return
		}
		_ = p.bw.Write(magicRespBytes) // NOTE: magic
		_ = p.bw.Write(mcr.respType.Bytes())
		_ = p.bw.Write(mcr.keyLen)
		_ = p.bw.Write(mcr.extraLen)
		_ = p.bw.Write(zeroBytes)
		if me := m.Err(); me != nil {
			_ = p.bw.Write(resopnseStatusInternalErrBytes)
		} else {
			_ = p.bw.Write(mcr.status)
		}
		_ = p.bw.Write(mcr.bodyLen)
		_ = p.bw.Write(mcr.opaque)
		err = p.bw.Write(mcr.cas)

		if err == nil && !bytes.Equal(mcr.bodyLen, zeroFourBytes) {
			err = p.bw.Write(mcr.data)
		}
	}
	return
}

func (p *proxyConn) Flush() (err error) {
	return p.bw.Flush()
}
