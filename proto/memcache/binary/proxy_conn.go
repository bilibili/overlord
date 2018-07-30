package binary

import (
	"bytes"
	"encoding/binary"

	"overlord/lib/bufio"
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
NEXTGET:
	// bufio reset buffer
	head, err := p.br.ReadExact(requestHeaderLen)
	if err == bufio.ErrBufferFull {
		return
	} else if err != nil {
		err = errors.Wrap(err, "MC decoder while reading text line")
		return
	}
	req := p.request(m)
	parseHeader(head, req, true)
	if err != nil {
		err = errors.Wrap(err, "MC decoder while parse header")
		return
	}
	switch req.rTp {
	case RequestTypeSet, RequestTypeAdd, RequestTypeReplace, RequestTypeGet, RequestTypeGetK,
		RequestTypeDelete, RequestTypeIncr, RequestTypeDecr, RequestTypeAppend, RequestTypePrepend, RequestTypeTouch, RequestTypeGat:
		if err = p.decodeCommon(m, req); err == bufio.ErrBufferFull {
			p.br.Advance(-requestHeaderLen)
			return
		}
		return
	case RequestTypeGetQ, RequestTypeGetKQ:
		if err = p.decodeCommon(m, req); err == bufio.ErrBufferFull {
			p.br.Advance(-requestHeaderLen)
			return
		}
		goto NEXTGET
	}
	err = errors.Wrap(ErrBadRequest, "MC decoder unsupport command")
	return
}

func (p *proxyConn) decodeCommon(m *proto.Message, req *MCRequest) (err error) {
	bl := binary.BigEndian.Uint32(req.bodyLen)
	body, err := p.br.ReadExact(int(bl))
	if err == bufio.ErrBufferFull {
		return
	} else if err != nil {
		err = errors.Wrap(err, "MC decodeCommon read exact body")
		return
	}
	el := uint8(req.extraLen[0])
	kl := binary.BigEndian.Uint16(req.keyLen)
	req.key = body[int(el) : int(el)+int(kl)]
	req.data = body
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
		req.rTp = RequestType(bs[1])
	}
	req.keyLen = bs[2:4]
	req.extraLen = bs[4:5]
	if !isDecode {
		req.status = bs[6:8]
	}
	req.bodyLen = bs[8:12]
	req.opaque = bs[12:16]
	req.cas = bs[16:24]
}

// Encode encode response and write into writer.
func (p *proxyConn) Encode(m *proto.Message) (err error) {
	reqs := m.Requests()
	for _, req := range reqs {
		mcr, ok := req.(*MCRequest)
		if !ok {
			err = errors.Wrap(ErrAssertReq, "MC Encoder assert request")
			return
		}
		_ = p.bw.Write(magicRespBytes) // NOTE: magic
		_ = p.bw.Write(mcr.rTp.Bytes())
		_ = p.bw.Write(mcr.keyLen)
		_ = p.bw.Write(mcr.extraLen)
		_ = p.bw.Write(zeroBytes)
		if err = m.Err(); err != nil {
			_ = p.bw.Write(resopnseStatusInternalErrBytes)
		} else {
			_ = p.bw.Write(mcr.status)
		}
		_ = p.bw.Write(mcr.bodyLen)
		_ = p.bw.Write(mcr.opaque)
		_ = p.bw.Write(mcr.cas)

		if err == nil && !bytes.Equal(mcr.bodyLen, zeroFourBytes) {
			_ = p.bw.Write(mcr.data)
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
