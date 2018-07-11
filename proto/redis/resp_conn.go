package redis

import (
	"overlord/lib/bufio"
	"overlord/lib/conv"
	libnet "overlord/lib/net"
)

// respConn will encode and decode resp object to socket
type respConn struct {
	br *bufio.Reader
	bw *bufio.Writer

	completed bool
}

// newRespConn will create new resp object Conn
func newRespConn(conn *libnet.Conn) *respConn {
	r := &respConn{
		br:        bufio.NewReader(conn, bufio.Get(1024)),
		bw:        bufio.NewWriter(conn),
		completed: true,
	}
	return r
}

// decodeMax will parse all the resp objects and keep the reuse reference until
// next call of this function.
func (rc *respConn) decodeMax(max int) (resps []*resp, err error) {
	var (
		robj *resp
	)

	if rc.completed {
		err = rc.br.Read()
		if err != nil {
			return nil, err
		}
		rc.completed = false
	}

	for i := 0; i < max; i++ {
		robj, err = rc.decodeResp()
		if err == bufio.ErrBufferFull {
			rc.completed = true
			err = nil
			return
		} else if err != nil {
			return
		}
		resps = append(resps, robj)
	}
	return
}

// decodeCount will trying to parse the buffer until meet the count.
func (rc *respConn) decodeCount(n int) (resps []*resp, err error) {
	var (
		robj  *resp
		begin = rc.br.Mark()
		now   = rc.br.Mark()
		i     = 0
	)

	for {
		// advance the r position to begin to avoid Read fill buffer
		rc.br.AdvanceTo(begin)
		err = rc.br.Read()
		if err != nil {
			return
		}
		rc.br.AdvanceTo(now)

		for {
			if i == n {
				return
			}

			robj, err = rc.decodeResp()
			if err == bufio.ErrBufferFull {
				break
			}
			if err != nil {
				return
			}
			resps = append(resps, robj)
			now = rc.br.Mark()
			i++
		}
	}
}

func (rc *respConn) decodeResp() (robj *resp, err error) {
	var (
		line []byte
		size int
	)
	line, err = rc.br.ReadLine()
	if err != nil {
		return nil, err
	}

	rtype := line[0]
	switch rtype {
	case respString, respInt, respError:
		// decocde use one line to parse
		robj = rc.decodePlain(rtype, line)
	case respBulk:
		// decode bulkString
		size, err = decodeInt(line[1 : len(line)-2])
		if err != nil {
			return
		}
		robj, err = rc.decodeBulk(size, len(line))
	case respArray:
		size, err = decodeInt(line[1 : len(line)-2])
		if err != nil {
			return
		}
		robj, err = rc.decodeArray(size, len(line))
	}
	return
}

func (rc *respConn) decodePlain(rtype byte, line []byte) *resp {
	return newRespPlain(rtype, line[0:len(line)-2])
}

func (rc *respConn) decodeBulk(size, lineSize int) (*resp, error) {
	if size == -1 {
		return newRespNull(respBulk), nil
	}
	data, err := rc.br.ReadExact(size + 2)
	if err == bufio.ErrBufferFull {
		rc.br.Advance(-(lineSize + size + 2))
	} else if err != nil {
		return nil, err
	}
	return newRespBulk(data[:len(data)-2]), nil
}

func (rc *respConn) decodeArray(size int, lineSize int) (*resp, error) {
	if size == -1 {
		return newRespNull(respArray), nil
	}
	robj := newRespArrayWithCapcity(size)
	mark := rc.br.Mark()
	for i := 0; i < size; i++ {
		sub, err := rc.decodeResp()
		if err != nil {
			rc.br.AdvanceTo(mark)
			rc.br.Advance(lineSize)
			return nil, err
		}
		robj.replace(i, sub)
	}
	return robj, nil
}

func decodeInt(data []byte) (int, error) {
	i, err := conv.Btoi(data)
	return int(i), err
}

func (rc *respConn) encode(robj *resp) error {
	switch robj.rtype {
	case respInt:
		return rc.encodeInt(robj)
	case respError:
		return rc.encodeError(robj)
	case respString:
		return rc.encodeString(robj)
	case respBulk:
		return rc.encodeBulk(robj)
	case respArray:
		return rc.encodeArray(robj)
	}
	return nil
}

// Flush was used to writev to flush.
func (rc *respConn) Flush() error {
	return rc.bw.Flush()
}

func (rc *respConn) encodeInt(robj *resp) (err error) {
	return rc.encodePlain(respIntBytes, robj)
}

func (rc *respConn) encodeError(robj *resp) (err error) {
	return rc.encodePlain(respErrorBytes, robj)
}

func (rc *respConn) encodeString(robj *resp) (err error) {
	return rc.encodePlain(respStringBytes, robj)
}

func (rc *respConn) encodePlain(rtypeBytes []byte, robj *resp) (err error) {
	err = rc.bw.Write(rtypeBytes)
	if err != nil {
		return
	}

	err = rc.bw.Write(robj.data)
	if err != nil {
		return
	}
	err = rc.bw.Write(crlfBytes)
	return
}

func (rc *respConn) encodeBulk(robj *resp) (err error) {
	// NOTICE: we need not to convert robj.Len() as int
	// due number has been writen into data
	err = rc.bw.Write(respBulkBytes)
	if err != nil {
		return
	}
	if robj.isNull() {
		err = rc.bw.Write(respNullBytes)
		return
	}

	err = rc.bw.Write(robj.data)
	if err != nil {
		return
	}

	err = rc.bw.Write(crlfBytes)
	return
}

func (rc *respConn) encodeArray(robj *resp) (err error) {
	err = rc.bw.Write(respArrayBytes)
	if err != nil {
		return
	}

	if robj.isNull() {
		err = rc.bw.Write(respNullBytes)
		return
	}
	// output size
	err = rc.bw.Write(robj.data)
	if err != nil {
		return
	}
	err = rc.bw.Write(crlfBytes)

	for _, item := range robj.slice() {
		err = rc.encode(item)
		if err != nil {
			return
		}
	}

	return
}
