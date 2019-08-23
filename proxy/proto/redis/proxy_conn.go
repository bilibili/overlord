package redis

import (
	"bytes"
	"fmt"
	"strconv"

	"overlord/pkg/bufio"
	"overlord/pkg/conv"
	libnet "overlord/pkg/net"
	"overlord/pkg/types"
	"overlord/proxy/proto"

	"github.com/pkg/errors"
)

var (
	nullBytes     = []byte("-1\r\n")
	okBytes       = []byte("OK\r\n")
	pongDataBytes = []byte("+PONG\r\n")
	justOkBytes   = []byte("+OK\r\n")
	//notSupportDataBytes = []byte("Error: command not support")
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

	authorized bool
	password   string
}

// NewProxyConn creates new redis Encoder and Decoder.
func NewProxyConn(conn *libnet.Conn, password string) proto.ProxyConn {
	r := &proxyConn{
		br:        bufio.NewReader(conn, bufio.Get(1024)),
		bw:        bufio.NewWriter(conn),
		completed: true,
		password:  password,
		resp:      &resp{},
	}
	if password != "" {
		r.authorized = false
	} else {
		r.authorized = true
	}
	return r
}

func (pc *proxyConn) Decode(msgs []*proto.Message) ([]*proto.Message, error) {
	var err error
	if pc.completed {
		if err = pc.br.Read(); err != nil {
			return nil, err
		}
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

func (pc *proxyConn) decode(msg *proto.Message) (err error) {
	// for migrate sync PING process
	for {
		mark := pc.br.Mark()
		if err = pc.resp.decode(pc.br); err != nil {
			if err == bufio.ErrBufferFull {
				pc.br.AdvanceTo(mark)
			}
			return
		}

		if pc.resp.arraySize != 0 {
			break
		}
	}

	if pc.resp.arraySize < 1 {
		r := nextReq(msg)
		r.resp.copy(pc.resp)
		return
	}
	conv.UpdateToUpper(pc.resp.array[0].data)
	cmd := pc.resp.array[0].data // NOTE: when array, first is command

	if bytes.Equal(cmd, cmdMSetBytes) {
		if pc.resp.arraySize%2 == 0 {
			err = ErrBadRequest
			return
		}
		mid := pc.resp.arraySize / 2 // Gets the number of command groups contained in the mset
		for i := 0; i < mid; i++ {
			r := nextReq(msg)
			r.mType = mergeTypeOK
			r.resp.reset() // NOTE: *3\r\n
			r.resp.respType = respArray
			r.resp.data = append(r.resp.data, arrayLenThree...)
			// array resp: mset
			nre1 := r.resp.next() // NOTE: $4\r\nMSET\r\n
			nre1.reset()
			nre1.respType = respBulk
			nre1.data = append(nre1.data, cmdMSetBytes...)
			// array resp: key
			nre2 := r.resp.next() // NOTE: $klen\r\nkey\r\n
			nre2.copy(pc.resp.array[i*2+1])
			// array resp: value
			nre3 := r.resp.next() // NOTE: $vlen\r\nvalue\r\n
			nre3.copy(pc.resp.array[i*2+2])
		}
	} else if bytes.Equal(cmd, cmdMGetBytes) {
		for i := 1; i < pc.resp.arraySize; i++ {
			r := nextReq(msg)
			r.mType = mergeTypeJoin
			r.resp.reset() // NOTE: *2\r\n
			r.resp.respType = respArray
			r.resp.data = append(r.resp.data, arrayLenTwo...)
			// array resp: get
			nre1 := r.resp.next() // NOTE: $3\r\nGET\r\n
			nre1.reset()
			nre1.respType = respBulk
			nre1.data = append(nre1.data, cmdGetBytes...)
			// array resp: key
			nre2 := r.resp.next() // NOTE: $klen\r\nkey\r\n
			nre2.copy(pc.resp.array[i])
		}
	} else if bytes.Equal(cmd, cmdDelBytes) || bytes.Equal(cmd, cmdExistsBytes) {
		for i := 1; i < pc.resp.arraySize; i++ {
			r := nextReq(msg)
			r.mType = mergeTypeCount
			r.resp.reset() // NOTE: *2\r\n
			r.resp.respType = respArray
			r.resp.data = append(r.resp.data, arrayLenTwo...)
			// array resp: get
			nre1 := r.resp.next() // NOTE: $3\r\nDEL\r\n | $6\r\nEXISTS\r\n
			nre1.copy(pc.resp.array[0])
			// array resp: key
			nre2 := r.resp.next() // NOTE: $klen\r\nkey\r\n
			nre2.copy(pc.resp.array[i])
		}
	} else {
		r := nextReq(msg)
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

func (pc *proxyConn) CmdCheck(m *proto.Message) (isSpecialDirective bool, err error) {
	isSpecialDirective = false

	if err = m.Err(); err != nil {
		se := errors.Cause(err).Error()
		pc.bw.Write(respErrorBytes)
		pc.bw.Write([]byte(se))
		pc.bw.Write(crlfBytes)
		return isSpecialDirective, err
	}
	req, ok := m.Request().(*Request)
	if !ok {
		return isSpecialDirective, ErrBadAssert
	}

	if !req.IsSupport() {
		err = pc.Bw().Write([]byte(fmt.Sprintf("-ERR unknown command `%s`, with args beginning with:\r\n", req.CmdString())))
		return isSpecialDirective, err
	}

	if !req.IsSpecial() {
		if !pc.authorized {
			err = pc.Bw().Write([]byte("-NOAUTH Authentication required.\r\n"))
			return isSpecialDirective, err
		}
		return isSpecialDirective, err
	}

	if req.IsSpecial() {
		reqData := req.resp.array[0].data
		if bytes.Equal(reqData, cmdAuthBytes) {
			if bytes.Equal(req.Key(), []byte(pc.password)) {
				pc.authorized = true
				err = pc.Bw().Write([]byte("+OK\r\n"))
			} else {
				err = pc.Bw().Write([]byte("-ERR invalid password\r\n"))
			}
			isSpecialDirective = true
		} else if bytes.Equal(reqData, cmdPingBytes) {
			if status := pc.authorized; status {
				err = pc.Bw().Write([]byte("+PONG\r\n"))
			} else {
				err = pc.Bw().Write([]byte("-NOAUTH Authentication required.\r\n"))
			}
			isSpecialDirective = true
		} else if bytes.Equal(reqData, cmdQuitBytes) {
			err = pc.Bw().Write([]byte("+OK\r\n"))
			isSpecialDirective = true
		} else if bytes.Equal(reqData, cmdCommandBytes) {
			err = pc.Bw().Write([]byte("-1\r\n"))
			isSpecialDirective = true
		}
	} else {
		if !pc.authorized {
			err = pc.Bw().Write([]byte("-NOAUTH Authentication required.\r\n"))
		}
	}

	return
}

func (pc *proxyConn) SetAuthorized(status bool) {
	pc.authorized = status
}

func (pc *proxyConn) GetAuthorized() bool {
	return pc.authorized
}

func (pc *proxyConn) SetPassword(password string) {
	pc.password = password
}

func (pc *proxyConn) GetPassword() string {
	return pc.password
}
