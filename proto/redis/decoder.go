package redis

import (
	"bufio"
	"errors"

	"bytes"

	"io"
	"strconv"

	"github.com/felixhao/overlord/proto"
)

// errors
var (
	ErrNotSupportRESPType = errors.New("-not support RESP type (+ ,- ,: ,$ ,* )")
)

type decoder struct {
	br *bufio.Reader
}

func (d *decoder) Decode() (*proto.Request, error) {
	respObj, err := d.decodeRespObj()
	if err != nil {
		return nil, err
	}

	req := &proto.Request{Type: proto.CacheTypeRedis}
	req.WithProto(newRRequest(respObj))
	return req, nil
}

func (d *decoder) decodeRespObj() (*resp, error) {
	rtypes, err := d.br.Peek(1)
	if err != nil {
		return nil, err
	}
	rtype := rtypes[0]

	switch rtype {
	case respString, respError, respInt:
		return d.decodeRespPlain()
	case respBulk:
		return d.decodeRespBulk()
	case respArray:
		return d.decodeRespArray()
	default:
		return nil, ErrNotSupportRESPType
	}
}

func (d *decoder) decodeRespPlain() (*resp, error) {
	rtype, err := d.br.ReadByte()
	if err != nil {
		return nil, err
	}
	data, err := d.readUntilCRLF()
	if err != nil {
		return nil, err
	}
	return newRespPlain(rtype, data), nil
}

func (d *decoder) decodeRespBulk() (*resp, error) {
	_, err := d.br.Discard(1)
	if err != nil {
		return nil, err
	}

	count, err := d.readCountAndDiscardCRLF()
	if err != nil {
		return nil, err
	}

	if count == -1 {
		return newRespBalk(nil), nil
	}

	data, err := d.readExact(count)
	if err != nil {
		return nil, err
	}
	err = d.discardCRLF()
	if err != nil {
		return nil, err
	}
	return newRespBalk(data), nil
}

func (d *decoder) decodeRespArray() (*resp, error) {
	_, err := d.br.Discard(1)
	if err != nil {
		return nil, err
	}
	count, err := d.readCountAndDiscardCRLF()
	if err != nil {
		return nil, err
	}

	if count == -1 {
		return newRespArray(nil), nil
	}

	resp := newRespArrayWithCapcity(count)
	for i := 0; i < count; i++ {
		sub, err := d.decodeRespObj()
		if err != nil {
			return nil, err
		}
		resp.replace(i, sub)
	}
	return resp, nil
}

func (d *decoder) readUntilCRLF() ([]byte, error) {
	var allBytes []byte
	for {
		data, err := d.br.ReadBytes(lfByte)
		if err != nil {
			return nil, err
		}
		allBytes = append(allBytes, data...)
		if bytes.HasSuffix(data, crlfBytes) {
			// ignore crlf
			break
		}
	}
	return allBytes[:len(allBytes)-2], nil
}

func (d *decoder) readExact(size int) ([]byte, error) {
	data := make([]byte, size)
	_, err := io.ReadFull(d.br, data)
	return data, err
}

func (d *decoder) discardCRLF() error {
	_, err := d.br.Discard(2)
	return err
}

func (d *decoder) readCountAndDiscardCRLF() (int, error) {
	countBytes, err := d.readUntilCRLF()
	if err != nil {
		return 0, err
	}
	ival, err := strconv.Atoi(string(countBytes))
	if err != nil {
		return 0, err
	}
	return ival, d.discardCRLF()
}
