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
		err = errors.WithStack(err)
		return
	}
	req := p.request(m)
	parseHeader(head, req, true)
	if err != nil {
		err = errors.WithStack(err)
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
	err = errors.Wrapf(ErrBadRequest, "MC decoder unsupport command:%x", req.rTp)
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
		req.rTp = RequestType(bs[1])
	}
	req.keyLen[0] = bs[2]
	req.keyLen[1] = bs[3]
	req.extraLen[0] = bs[4]
	if !isDecode {
		req.status[0] = bs[6]
		req.status[1] = bs[7]
	}
	req.bodyLen[0] = bs[8]
	req.bodyLen[1] = bs[9]
	req.bodyLen[2] = bs[10]
	req.bodyLen[3] = bs[11]
	req.opaque[0] = bs[12]
	req.opaque[1] = bs[13]
	req.opaque[2] = bs[14]
	req.opaque[3] = bs[15]
	req.cas[0] = bs[16]
	req.cas[1] = bs[17]
	req.cas[2] = bs[18]
	req.cas[3] = bs[19]
	req.cas[4] = bs[20]
	req.cas[5] = bs[21]
	req.cas[6] = bs[22]
	req.cas[7] = bs[23]
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
	return p.bw.Flush()
}
