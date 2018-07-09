package redis

import (
	"overlord/lib/bufio"
	"overlord/lib/conv"
	"overlord/proto"
)

type redisConn struct {
	br        bufio.Reader
	completed bool

	bw bufio.Writer
}

func (rc *redisConn) Decode(msgs []*proto.Message) ([]*proto.Message, error) {
	var err error
	if rc.completed {
		err = rc.br.Read()
		if err != nil {
			return nil, err
		}
	}
	for i := range msgs {
		rc.completed = false
		// set msg type
		msgs[i].Type = proto.CacheTypeRedis
		// decode
		err = rc.decode(msgs[i])
		if err == bufio.ErrBufferFull {
			rc.completed = true
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

func (rc *redisConn) decode(msg *proto.Message) (err error) {
	robj, err := rc.decodeResp()
	if err != nil {
		return
	}
	if isComplex(robj.nth(0).data) {
		cmds, inerr := newSubCmd(robj)
		if inerr != nil {
			err = inerr
			return
		}
		for _, cmd := range cmds {
			msg.WithRequest(cmd)
		}
	} else {
		msg.WithRequest(newCommand(robj))
	}
	return
}

func (rc *redisConn) decodeResp() (robj *resp, err error) {
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

func (rc *redisConn) decodePlain(rtype byte, line []byte) *resp {
	return newRespPlain(rtype, line[0:len(line)-2])
}

func (rc *redisConn) decodeBulk(size, lineSize int) (*resp, error) {
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

func (rc *redisConn) decodeArray(size int, lineSize int) (*resp, error) {
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

func (rc *redisConn) Encode(msg *proto.Message) error {
	return nil
}

func decodeInt(data []byte) (int, error) {
	i, err := conv.Btoi(data)
	return int(i), err
}
