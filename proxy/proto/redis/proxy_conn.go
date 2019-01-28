package redis

import (
	"bytes"
	"strconv"

	"overlord/pkg/bufio"
	"overlord/pkg/conv"
	libnet "overlord/pkg/net"
	"overlord/pkg/types"
	"overlord/proxy/proto"

	"github.com/pkg/errors"
)

var (
	nullBytes           = []byte("-1\r\n")
	okBytes             = []byte("OK\r\n")
	pongDataBytes       = []byte("PONG")
	justOkBytes         = []byte("OK")
	notSupportDataBytes = []byte("Error: command not support")
)

// ProxyConn is export for redis cluster.
type ProxyConn = proxyConn

// Bw return proxyConn Writer.
func (pc *ProxyConn) Bw() *bufio.Writer {
	return pc.bw
}

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
		if err = pc.br.Read(); err != nil {
			return nil, err
		}

		pc.rbytes = pc.br.GetReadedSize()
		pc.completed = false
	}
	for i := range msgs {
		msgs[i].Type = types.CacheTypeRedis
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
	mark := pc.br.Mark()
	if err = pc.resp.decode(pc.br); err != nil {
		if err == bufio.ErrBufferFull {
			pc.br.AdvanceTo(mark)
		}
		return
	}
	if pc.resp.arrayn < 1 {
		r := nextReq(m)
		r.resp.copy(pc.resp)
		return
	}
	conv.UpdateToUpper(pc.resp.array[0].data)
	cmd := pc.resp.array[0].data // NOTE: when array, first is command
	if bytes.Equal(cmd, cmdMSetBytes) {
		if pc.resp.arrayn%2 == 0 {
			err = ErrBadRequest
			return
		}
		mid := pc.resp.arrayn / 2
		for i := 0; i < mid; i++ {
			r := nextReq(m)
			r.mType = mergeTypeOK
			r.resp.reset() // NOTE: *3\r\n
			r.resp.rTp = respArray
			r.resp.data = append(r.resp.data, arrayLenThree...)
			// array resp: mset
			nre1 := r.resp.next() // NOTE: $4\r\nMSET\r\n
			nre1.reset()
			nre1.rTp = respBulk
			nre1.data = append(nre1.data, cmdMSetBytes...)
			// array resp: key
			nre2 := r.resp.next() // NOTE: $klen\r\nkey\r\n
			nre2.copy(pc.resp.array[i*2+1])
			// array resp: value
			nre3 := r.resp.next() // NOTE: $vlen\r\nvalue\r\n
			nre3.copy(pc.resp.array[i*2+2])
		}
	} else if bytes.Equal(cmd, cmdMGetBytes) {
		for i := 1; i < pc.resp.arrayn; i++ {
			r := nextReq(m)
			r.mType = mergeTypeJoin
			r.resp.reset() // NOTE: *2\r\n
			r.resp.rTp = respArray
			r.resp.data = append(r.resp.data, arrayLenTwo...)
			// array resp: get
			nre1 := r.resp.next() // NOTE: $3\r\nGET\r\n
			nre1.reset()
			nre1.rTp = respBulk
			nre1.data = append(nre1.data, cmdGetBytes...)
			// array resp: key
			nre2 := r.resp.next() // NOTE: $klen\r\nkey\r\n
			nre2.copy(pc.resp.array[i])
		}
	} else if bytes.Equal(cmd, cmdDelBytes) || bytes.Equal(cmd, cmdExistsBytes) {
		for i := 1; i < pc.resp.arrayn; i++ {
			r := nextReq(m)
			r.mType = mergeTypeCount
			r.resp.reset() // NOTE: *2\r\n
			r.resp.rTp = respArray
			r.resp.data = append(r.resp.data, arrayLenTwo...)
			// array resp: get
			nre1 := r.resp.next() // NOTE: $3\r\nDEL\r\n | $6\r\nEXISTS\r\n
			nre1.copy(pc.resp.array[0])
			// array resp: key
			nre2 := r.resp.next() // NOTE: $klen\r\nkey\r\n
			nre2.copy(pc.resp.array[i])
		}
	} else {
		r := nextReq(m)
		r.resp.copy(pc.resp)
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
	r.mType = mergeTypeNo
	return r
}

func (pc *proxyConn) Encode(m *proto.Message) (err error) {
	if err = m.Err(); err != nil {
		se := errors.Cause(err).Error()
		pc.bw.Write(respErrorBytes)
		pc.bw.Write([]byte(se))
		pc.bw.Write(crlfBytes)
		return
	}
	req, ok := m.Request().(*Request)
	if !ok {
		return ErrBadAssert
	}
	switch req.mType {
	case mergeTypeOK:
		err = pc.mergeOK(m)
	case mergeTypeJoin:
		err = pc.mergeJoin(m)
	case mergeTypeCount:
		err = pc.mergeCount(m)
	default:
		if !req.IsSupport() {
			req.reply.rTp = respError
			req.reply.data = req.reply.data[:0]
			req.reply.data = append(req.reply.data, notSupportDataBytes...)
		} else if req.IsCtl() {
			reqData := req.resp.array[0].data
			if bytes.Equal(reqData, cmdPingBytes) {
				req.reply.rTp = respString
				req.reply.data = req.reply.data[:0]
				req.reply.data = append(req.reply.data, pongDataBytes...)
			} else if bytes.Equal(reqData, cmdQuitBytes) {
				req.reply.rTp = respString
				req.reply.data = req.reply.data[:0]
				req.reply.data = append(req.reply.data, justOkBytes...)
			}
		}
		err = req.reply.encode(pc.bw)
	}
	if err != nil {
		err = errors.WithStack(err)
	}
	return
}

func (pc *proxyConn) mergeOK(m *proto.Message) (err error) {
	_ = pc.bw.Write(respStringBytes)
	err = pc.bw.Write(okBytes)
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
	err = pc.bw.Write(crlfBytes)
	return
}

func (pc *proxyConn) mergeJoin(m *proto.Message) (err error) {
	reqs := m.Requests()
	_ = pc.bw.Write(respArrayBytes)
	if len(reqs) == 0 {
		err = pc.bw.Write(nullBytes)
		return
	}
	_ = pc.bw.Write([]byte(strconv.Itoa(len(reqs))))
	if err = pc.bw.Write(crlfBytes); err != nil {
		return
	}
	for _, mreq := range reqs {
		req, ok := mreq.(*Request)
		if !ok {
			return ErrBadAssert
		}
		if err = req.reply.encode(pc.bw); err != nil {
			return
		}
	}
	return
}

func (pc *proxyConn) Flush() (err error) {
	return pc.bw.Flush()
}
