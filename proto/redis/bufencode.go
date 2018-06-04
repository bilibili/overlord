package redis

import "strconv"

func (b *buffer) encodeResp(respObj *resp) error {
	switch respObj.rtype {
	case respString, respError, respInt:
		return b.encodeRespPlain(respObj)
	case respBulk:
		return b.encodeRespBulk(respObj)
	case respArray:
		return b.encodeRespArray(respObj)
	default:
		return ErrNotSupportRESPType
	}
}

func (b *buffer) encodeRespPlain(respObj *resp) error {
	err := b.writeRespType(respObj.rtype)
	if err != nil {
		return err
	}

	_, err = b.bw.Write(respObj.data)
	if err != nil {
		return err
	}
	_, err = b.bw.Write(crlfBytes)
	return err
}

func (b *buffer) encodeRespBulk(respObj *resp) error {
	err := b.writeRespType(respObj.rtype)
	if err != nil {
		return err
	}

	if respObj.isNull() {
		_, err := b.bw.Write(nullBytes)
		return err
	}

	size := len(respObj.data)
	err = b.writeIntWithCRLF(size)
	if err != nil {
		return err
	}

	_, err = b.bw.Write(respObj.data)
	if err != nil {
		return err
	}

	err = b.writeCRLF()
	return err
}

func (b *buffer) encodeRespArray(respObj *resp) error {
	err := b.writeRespType(respObj.rtype)
	if err != nil {
		return err
	}

	if respObj.isNull() {
		_, err = b.bw.Write(nullBytes)
		return err
	}
	count := len(respObj.array)

	err = b.writeIntWithCRLF(count)
	if err != nil {
		return err
	}

	for _, sub := range respObj.array {
		err := b.encodeResp(sub)
		if err != nil {
			return err
		}
	}
	return nil
}

func (b *buffer) writeIntWithCRLF(num int) error {
	bs := strconv.Itoa(num)
	_, err := b.bw.Write([]byte(bs))
	if err != nil {
		return err
	}
	return b.writeCRLF()
}

func (b *buffer) writeCRLF() error {
	_, err := b.bw.Write(crlfBytes)
	return err
}

func (b *buffer) writeRespType(rtype respType) error {
	return b.bw.WriteByte(rtype)
}

func (b *buffer) Flush() error {
	return b.bw.Flush()
}
