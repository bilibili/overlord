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

func (pc *proxyConn) Decode(msgs []*proto.Message) ([]*proto.Message, error) {
	var err error
	// if completed, means that we have parsed all the buffered
	// if not completed, we need only to parse the buffered message
	if pc.completed {
		err = pc.br.Read()
		if err != nil {
			return nil, err
		}
	}
	for i := range msgs {
		pc.completed = false
		// set msg type
		msgs[i].Type = types.CacheTypeMemcacheBinary
		// decode
		err = pc.decode(msgs[i])
		if err == bufio.ErrBufferFull {
			pc.completed = true
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

func (pc *proxyConn) decode(m *proto.Message) (err error) {
NEXTGET:
	// bufio reset buffer
	head, err := pc.br.ReadExact(requestHeaderLen)
	if err == bufio.ErrBufferFull {
		return
	} else if err != nil {
		err = errors.WithStack(err)
		return
	}
	req := pc.request(m)
	parseHeader(head, req, true)
	if err != nil {
		err = errors.WithStack(err)
		return
	}
	switch req.respType {
	case RequestTypeSet, RequestTypeAdd, RequestTypeReplace, RequestTypeGet, RequestTypeGetK,
		RequestTypeDelete, RequestTypeIncr, RequestTypeDecr, RequestTypeAppend, RequestTypePrepend, RequestTypeTouch, RequestTypeGat:
		if err = pc.decodeCommon(m, req); err == bufio.ErrBufferFull {
			pc.br.Advance(-requestHeaderLen)
			return
		}
		return
	case RequestTypeGetQ, RequestTypeGetKQ:
		if err = pc.decodeCommon(m, req); err == bufio.ErrBufferFull {
			pc.br.Advance(-requestHeaderLen)
			return
		}
		goto NEXTGET
	}
	err = errors.Wrapf(ErrBadRequest, "MC decoder unsupport command:%x", req.respType)
	return
}

func (pc *proxyConn) decodeCommon(m *proto.Message, req *MCRequest) (err error) {
	bl := binary.BigEndian.Uint32(req.bodyLen)
	body, err := pc.br.ReadExact(int(bl))
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

func (pc *proxyConn) request(m *proto.Message) *MCRequest {
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
func (pc *proxyConn) Encode(m *proto.Message) (err error) {
	reqs := m.Requests()
	for _, req := range reqs {
		mcr, ok := req.(*MCRequest)
		if !ok {
			err = errors.WithStack(ErrAssertReq)
			return
		}
		_ = pc.bw.Write(magicRespBytes) // NOTE: magic
		_ = pc.bw.Write(mcr.respType.Bytes())
		_ = pc.bw.Write(mcr.keyLen)
		_ = pc.bw.Write(mcr.extraLen)
		_ = pc.bw.Write(zeroBytes)
		if me := m.Err(); me != nil {
			_ = pc.bw.Write(resopnseStatusInternalErrBytes)
		} else {
			_ = pc.bw.Write(mcr.status)
		}
		_ = pc.bw.Write(mcr.bodyLen)
		_ = pc.bw.Write(mcr.opaque)
		err = pc.bw.Write(mcr.cas)

		if err == nil && !bytes.Equal(mcr.bodyLen, zeroFourBytes) {
			err = pc.bw.Write(mcr.data)
		}
	}
	return
}

func (pc *proxyConn) Flush() (err error) {
	return pc.bw.Flush()
}

func (pc *proxyConn) IsAuthorized() bool {
	return true
}

func (pc *proxyConn) CmdCheck(m *proto.Message) (isSpecialCmd bool, err error) {
	return false, err
}
