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

// decodeMsg will parse all the req resp objects into message and keep the reuse reference until
// next call of this function.
func (rc *respConn) decodeMsg(msgs []*proto.Message) ([]*proto.Message, error) {
	var err error
	if rc.completed {
		err = rc.br.Read()
		if err != nil {
			return nil, err
		}
		rc.completed = false
	}

	for i, msg := range msgs {
		var robj *resp
		req := msg.Request()
		if req == nil {
			robj = respPool.Get().(*resp)
		} else {
			robj = req.(*Command).respObj
			// reset metadata before reuse it.
			robj.reset()
		}
		err = rc.decodeRESP(robj)
		if err == bufio.ErrBufferFull {
			rc.completed = true
			return msgs[:i], nil
		} else if err != nil {
			return nil, err
		}
		msg.Type = proto.CacheTypeRedis
		msg.MarkStart()
		err = robj.decode(msg)
		if err != nil {
			msg.Reset()
			return nil, err
		}
	}
	return msgs, nil
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

// decodeOne will trying to parse the buffer until get one complete resp..
func (rc *respConn) decodeOne(robj *resp) (err error) {
	var (
		begin = rc.br.Mark()
	)
	for {
		// advance the r position to begin to avoid Read fill buffer
		rc.br.AdvanceTo(begin)
		err = rc.br.Read()
		if err != nil {
			return
		}
		err = rc.decodeRESP(robj)
		if err == bufio.ErrBufferFull {
			continue
		}
		return
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

// Flush was used to writev to flush.
func (rc *respConn) Flush() error {
	return rc.bw.Flush()
}
