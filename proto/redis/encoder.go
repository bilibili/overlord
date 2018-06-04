package redis

import (
	"bufio"
	"errors"
	"strconv"

	"github.com/felixhao/overlord/proto"
)

// errors
var (
	ErrMissMatchResponseType = errors.New("response type miss match, except: CacheTypeRedis")
)

var (
	nullBytes = []byte("-1\r\n")
)

type encoder struct {
	bw *bufio.Writer
}

func (e *encoder) Encode(response *proto.Response) error {
	if response.Type != proto.CacheTypeRedis {
		return ErrMissMatchResponseType
	}
	r, ok := response.Proto().(*RResponse)
	if !ok {
		return ErrMissMatchResponseType
	}
	err := e.encodeResp(r.respObj)
	if err != nil {
		return err
	}
	return e.bw.Flush()
}

func (e *encoder) encodeResp(respObj *resp) error {
	switch respObj.rtype {
	case respString, respError, respInt:
		return e.encodeRespPlain(respObj)
	case respBulk:
		return e.encodeRespBulk(respObj)
	case respArray:
		return e.encodeRespArray(respObj)
	default:
		return ErrNotSupportRESPType
	}
}

func (e *encoder) encodeRespPlain(respObj *resp) error {
	err := e.writeRespType(respObj.rtype)
	if err != nil {
		return err
	}

	_, err = e.bw.Write(respObj.data)
	if err != nil {
		return err
	}
	_, err = e.bw.Write(crlfBytes)
	return err
}

func (e *encoder) encodeRespBulk(respObj *resp) error {
	err := e.writeRespType(respObj.rtype)
	if err != nil {
		return err
	}

	if respObj.isNull() {
		_, err := e.bw.Write(nullBytes)
		return err
	}

	size := len(respObj.data)
	err = e.writeIntWithCRLF(size)
	if err != nil {
		return err
	}

	_, err = e.bw.Write(respObj.data)
	if err != nil {
		return err
	}

	err = e.writeCRLF()
	return err
}

func (e *encoder) encodeRespArray(respObj *resp) error {
	err := e.writeRespType(respObj.rtype)
	if err != nil {
		return err
	}

	if respObj.isNull() {
		_, err = e.bw.Write(nullBytes)
		return err
	}
	count := len(respObj.array)

	err = e.writeIntWithCRLF(count)
	if err != nil {
		return err
	}

	for _, sub := range respObj.array {
		err := e.encodeResp(sub)
		if err != nil {
			return err
		}
	}
	return nil
}

func (e *encoder) writeIntWithCRLF(num int) error {
	bs := strconv.Itoa(num)
	_, err := e.bw.Write([]byte(bs))
	if err != nil {
		return err
	}
	return e.writeCRLF()
}

func (e *encoder) writeCRLF() error {
	_, err := e.bw.Write(crlfBytes)
	return err
}

func (e *encoder) writeRespType(rtype respType) error {
	return e.bw.WriteByte(rtype)
}
