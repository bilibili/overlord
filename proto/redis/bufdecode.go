package redis

import (
	"bytes"
	"io"
	"strconv"
)

func (b *buffer) decodeRespObj() (*resp, error) {
	rtypes, err := b.br.Peek(1)
	if err != nil {
		return nil, err
	}
	rtype := rtypes[0]

	switch rtype {
	case respString, respError, respInt:
		return b.decodeRespPlain()
	case respBulk:
		return b.decodeRespBulk()
	case respArray:
		return b.decodeRespArray()
	default:
		return nil, ErrNotSupportRESPType
	}
}

func (b *buffer) decodeRespPlain() (*resp, error) {
	rtype, err := b.br.ReadByte()
	if err != nil {
		return nil, err
	}
	data, err := b.readUntilCRLF()
	if err != nil {
		return nil, err
	}
	return newRespPlain(rtype, data), nil
}

func (b *buffer) decodeRespBulk() (*resp, error) {
	_, err := b.br.Discard(1)
	if err != nil {
		return nil, err
	}

	count, err := b.readCountAndDiscardCRLF()
	if err != nil && err != io.EOF {
		return nil, err
	}

	if count == -1 {
		return newRespBulk(nil), err
	}

	data, err := b.readExact(count)
	if err != nil {
		return nil, err
	}
	err = b.discardCRLF()
	return newRespBulk(data), err
}

func (b *buffer) decodeRespArray() (*resp, error) {
	_, err := b.br.Discard(1)
	if err != nil {
		return nil, err
	}
	count, err := b.readCountAndDiscardCRLF()
	if err != nil {
		return nil, err
	}

	if count == -1 {
		return newRespArray(nil), nil
	}

	var sub *resp
	resp := newRespArrayWithCapcity(count)
	for i := 0; i < count; i++ {
		sub, err = b.decodeRespObj()
		if err != nil {
			if i != count-1 || err == io.EOF {
				return nil, err
			}
		}
		resp.replace(i, sub)
	}
	return resp, err
}

func (b *buffer) readUntilCRLF() ([]byte, error) {
	var allBytes []byte
	for {
		data, err := b.br.ReadBytes(lfByte)
		if err != nil && err != io.EOF {
			return nil, err
		}
		allBytes = append(allBytes, data...)
		if bytes.HasSuffix(data, crlfBytes) {
			break
		}
	}
	// ignore crlf
	return allBytes[:len(allBytes)-2], nil
}

func (b *buffer) readExact(size int) ([]byte, error) {
	data := make([]byte, size)
	_, err := io.ReadFull(b.br, data)
	return data, err
}

func (b *buffer) discardCRLF() error {
	_, err := b.br.Discard(2)
	return err
}

func (b *buffer) readCountAndDiscardCRLF() (int, error) {
	countBytes, err := b.readUntilCRLF()
	if err != nil {
		return 0, err
	}
	return strconv.Atoi(string(countBytes))
}
