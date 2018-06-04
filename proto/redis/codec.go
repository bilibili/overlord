package redis

import (
	"github.com/felixhao/overlord/proto"
)

type encoder struct {
	buf *buffer
}

func (e *encoder) Encode(response *proto.Response) error {
	if response.Type != proto.CacheTypeRedis {
		return ErrMissMatchResponseType
	}
	r, ok := response.Proto().(*RResponse)
	if !ok {
		return ErrMissMatchResponseType
	}
	err := e.buf.encodeResp(r.respObj)
	if err != nil {
		return err
	}
	return e.buf.Flush()
}

type decoder struct {
	buf *buffer
}

func (d *decoder) Decode() (*proto.Request, error) {
	respObj, err := d.buf.decodeRespObj()
	if err != nil {
		return nil, err
	}

	req := &proto.Request{Type: proto.CacheTypeRedis}
	req.WithProto(newRRequest(respObj))
	return req, nil
}
