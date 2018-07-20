package redis

import (
	"overlord/lib/bufio"
	"overlord/lib/conv"
	libnet "overlord/lib/net"
	"overlord/proto"
)

// respConn will encode and decode res object to socket
type respConn struct {
	br *bufio.Reader
	bw *bufio.Writer

	completed bool
}

// newRespConn will create new redis server protocol object Conn
func newRESPConn(conn *libnet.Conn) *respConn {
	r := &respConn{
		br:        bufio.NewReader(conn, bufio.Get(1024)),
		bw:        bufio.NewWriter(conn),
		completed: true,
	}
	return r
}

// decodeMax will parse all the resp objects and keep the reuse reference until
// next call of this function.
func (rc *respConn) decodeMax(msgs []*proto.Message) (resps []*resp, err error) {
	if rc.completed {
		err = rc.br.Read()
		if err != nil {
			return
		}
		rc.completed = false
	}

	for _, msg := range msgs {
		var robj *resp
		subcmd, ok := msg.Request().(*Command)
		if !ok {
			robj = respPool.Get().(*resp)
		} else {
			robj = subcmd.respObj
		}
		err = rc.decodeRESP(robj)
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
func (rc *respConn) decodeCount(robjs []*resp) (err error) {
	var (
		begin = rc.br.Mark()
		now   = rc.br.Mark()
		n     = len(robjs)
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

			err = rc.decodeRESP(robjs[i])
			if err == bufio.ErrBufferFull {
				break
			}
			if err != nil {
				return
			}
			now = rc.br.Mark()
			i++
		}
	}
}

func (rc *respConn) decodeRESP(robj *resp) (err error) {
	var (
		line []byte
	)
	line, err = rc.br.ReadLine()
	if err != nil {
		return err
	}

	rtype := line[0]
	switch rtype {
	case respString, respInt, respError:
		// decocde use one line to parse
		rc.decodePlain(line, robj)
	case respBulk:
		// decode bulkString
		// fmt.Printf("line:%s\n", strconv.Quote(string(line)))
		err = rc.decodeBulk(line, robj)
	case respArray:
		err = rc.decodeArray(line, robj)
	}
	return
}

func (rc *respConn) decodePlain(line []byte, robj *resp) {
	robj.setPlain(line[0], line[1:len(line)-2])
}

func (rc *respConn) decodeBulk(line []byte, robj *resp) error {
	lineSize := len(line)
	sizeBytes := line[1 : lineSize-2]
	// fmt.Printf("size:%s\n", strconv.Quote(string(sizeBytes)))
	size, err := decodeInt(sizeBytes)
	if err != nil {
		return err
	}
	if size == -1 {
		robj.setNull(respBulk)
		return nil
	}
	rc.br.Advance(-(lineSize - 1))
	fullDataSize := lineSize - 1 + size + 2
	data, err := rc.br.ReadExact(fullDataSize)
	// fmt.Printf("data:%s\n", strconv.Quote(string(data)))
	if err == bufio.ErrBufferFull {
		rc.br.Advance(-1)
		return err
	} else if err != nil {
		return err
	}
	robj.setBulk(data[:len(data)-2])
	return nil
}

func (rc *respConn) decodeArray(line []byte, robj *resp) error {
	lineSize := len(line)
	size, err := decodeInt(line[1 : lineSize-2])
	if err != nil {
		return err
	}
	if size == -1 {
		robj.setNull(respArray)
		return nil
	}
	robj.data = line[1 : lineSize-2]
	robj.rtype = respArray
	mark := rc.br.Mark()
	for i := 0; i < size; i++ {
		err = rc.decodeRESP(robj.next())
		if err != nil {
			rc.br.AdvanceTo(mark)
			rc.br.Advance(-lineSize)
			return err
		}
	}
	return nil
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
