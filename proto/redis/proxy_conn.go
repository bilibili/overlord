package redis

import (
	"bytes"
	"strconv"

	"overlord/lib/bufio"
	"overlord/lib/conv"
	libnet "overlord/lib/net"
	"overlord/proto"

	"github.com/pkg/errors"
)

type proxyConn struct {
	br        *bufio.Reader
	bw        *bufio.Writer
	completed bool

	resp *resp
}

// NewProxyConn creates new redis Encoder and Decoder.
func NewProxyConn(conn *libnet.Conn) proto.ProxyConn {
	r := &proxyConn{
		br:        bufio.NewReader(conn, bufio.Get(1024)),
		bw:        bufio.NewWriter(conn),
		completed: true,
		resp:      &resp{},
	}
	return r
}

func (pc *proxyConn) Decode(msgs []*proto.Message) ([]*proto.Message, error) {
	var err error
	if pc.completed {
		err = pc.br.Read()
		if err != nil {
			return nil, err
		}
		pc.completed = false
	}
	for i := range msgs {
		msgs[i].Type = proto.CacheTypeRedis
		// decode
		if err = pc.decode(msgs[i]); err == bufio.ErrBufferFull {
			pc.completed = true
			return msgs[:i], nil
		} else if err != nil {
			return nil, err
		}
		msgs[i].MarkStart()
	}
	return msgs, nil
}

func (pc *proxyConn) decode(m *proto.Message) (err error) {
	if err = pc.resp.decode(pc.br); err != nil {
		return
	}
	if pc.resp.arrayn < 1 {
		return
	}
	cmd := pc.resp.array[0].data // NOTE: when array, first is command
	if bytes.Equal(cmd, cmdMSetBytes) {
		mid := pc.resp.arrayn / 2
		for i := 0; i < mid; i++ {
			r := nextReq(m)
			r.mType = mergeTypeOK
			r.resp.reset() // NOTE: *3\r\n
			r.resp.rTp = respArray
			r.resp.data = arrayLenThree
			// array resp: mset
			nre1 := r.resp.next() // NOTE: $4\r\nMSET\r\n
			nre1.reset()
			nre1.rTp = respBulk
			nre1.data = cmdMSetBytes
			// array resp: key
			nre2 := r.resp.next() // NOTE: $klen\r\nkey\r\n
			nre2.reset()
			nre2.rTp = pc.resp.array[i*2+1].rTp
			nre2.data = pc.resp.array[i*2+1].data
			// array resp: value
			nre3 := r.resp.next() // NOTE: $vlen\r\nvalue\r\n
			nre3.reset()
			nre3.rTp = pc.resp.array[i*2+2].rTp
			nre3.data = pc.resp.array[i*2+2].data
		}
	} else if bytes.Equal(cmd, cmdMGetBytes) {
		for i := 1; i < pc.resp.arrayn; i++ {
			r := nextReq(m)
			r.mType = mergeTypeJoin
			r.resp.reset() // NOTE: *2\r\n
			r.resp.rTp = respArray
			r.resp.data = arrayLenTwo
			// array resp: get
			nre1 := r.resp.next() // NOTE: $3\r\nGET\r\n
			nre1.reset()
			nre1.rTp = respBulk
			nre1.data = cmdGetBytes
			// array resp: key
			nre2 := r.resp.next() // NOTE: $klen\r\nkey\r\n
			nre2.reset()
			nre2.rTp = pc.resp.array[i].rTp
			nre2.data = pc.resp.array[i].data
		}
	} else if bytes.Equal(cmd, cmdDelBytes) || bytes.Equal(cmd, cmdExistsBytes) {
		for i := 1; i < pc.resp.arrayn; i++ {
			r := nextReq(m)
			r.mType = mergeTypeCount
			r.resp.reset() // NOTE: *2\r\n
			r.resp.rTp = respArray
			r.resp.data = arrayLenTwo
			// array resp: get
			nre1 := r.resp.next() // NOTE: $3\r\nDEL\r\n | $6\r\nEXISTS\r\n
			nre1.reset()
			nre1.rTp = pc.resp.array[0].rTp
			nre1.data = pc.resp.array[0].data
			// array resp: key
			nre2 := r.resp.next() // NOTE: $klen\r\nkey\r\n
			nre2.reset()
			nre2.rTp = pc.resp.array[i].rTp
			nre2.data = pc.resp.array[i].data
		}
	} else {
		r := nextReq(m)
		r.resp.reset()
		r.resp.rTp = pc.resp.rTp
		r.resp.data = pc.resp.data
		for i := 0; i < pc.resp.arrayn; i++ {
			nre := r.resp.next()
			nre.reset()
			nre.rTp = pc.resp.array[i].rTp
			nre.data = pc.resp.array[i].data
		}
	}
	return
}

func nextReq(m *proto.Message) *Request {
	req := m.NextReq()
	if req == nil {
		r := getReq()
		m.WithRequest(r)
		return r
	}
	r := req.(*Request)
	return r
}

func (pc *proxyConn) Encode(m *proto.Message) (err error) {
	if err = m.Err(); err != nil {
		return
	}
	req, ok := m.Request().(*Request)
	if !ok {
		return ErrBadAssert
	}
	if !m.IsBatch() {
		err = req.reply.encode(pc.bw)
	} else {
		switch req.mType {
		case mergeTypeOK:
			err = pc.mergeOK(m)
		case mergeTypeJoin:
			err = pc.mergeJoin(m)
		case mergeTypeCount:
			err = pc.mergeCount(m)
		default:
			// TODO: panic???
		}
	}
	if err != nil {
		err = errors.Wrap(err, "Redis Encoder before flush response")
		return
	}
	if err = pc.bw.Flush(); err != nil {
		err = errors.Wrap(err, "Redis Encoder flush response")
	}
	return
}

func (pc *proxyConn) mergeOK(m *proto.Message) (err error) {
	_ = pc.bw.Write(respStringBytes)
	_ = pc.bw.Write(okBytes)
	_ = pc.bw.Write(crlfBytes)
	return
}

func (pc *proxyConn) mergeCount(m *proto.Message) (err error) {
	var sum = 0
	for _, mreq := range m.Requests() {
		req, ok := mreq.(*Request)
		if !ok {
			return ErrBadAssert
		}
		ival, err := conv.Btoi(req.reply.data)
		if err != nil {
			return ErrBadCount
		}
		sum += int(ival)
	}
	_ = pc.bw.Write(respIntBytes)
	_ = pc.bw.Write([]byte(strconv.Itoa(sum)))
	_ = pc.bw.Write(crlfBytes)
	return
}

func (pc *proxyConn) mergeJoin(m *proto.Message) (err error) {
	reqs := m.Requests()
	_ = pc.bw.Write(respArrayBytes)
	if len(reqs) == 0 {
		_ = pc.bw.Write(respNullBytes)
		return
	}
	_ = pc.bw.Write([]byte(strconv.Itoa(len(reqs))))
	_ = pc.bw.Write(crlfBytes)
	for _, mreq := range reqs {
		req, ok := mreq.(*Request)
		if !ok {
			return ErrBadAssert
		}
		_ = req.reply.encode(pc.bw)
	}
	return
}
