package redis

import (
	"bytes"
	stderrs "errors"
	"overlord/lib/conv"
	libnet "overlord/lib/net"
	"overlord/proto"
	"strconv"

	"github.com/pkg/errors"
)

// errors
var (
	ErrBadAssert = stderrs.New("bad assert for redis")
	ErrBadCount  = stderrs.New("bad count number")
)

var (
	robjErrNotSupport = newRESPBulk([]byte("Error: command not support"))
	robjPong          = newRESPPlain(respString, pongBytes)
)

type proxyConn struct {
	rc *respConn
}

// NewProxyConn creates new redis Encoder and Decoder.
func NewProxyConn(conn *libnet.Conn) proto.ProxyConn {
	r := &proxyConn{
		rc: newRESPConn(conn),
	}
	return r
}

func (pc *proxyConn) Decode(msgs []*proto.Message) ([]*proto.Message, error) {
	return pc.rc.decodeMsg(msgs)
}

func (pc *proxyConn) Encode(msg *proto.Message) (err error) {
	if err := msg.Err(); err != nil {
		return pc.encodeError(err)
	}
	err = pc.merge(msg)
	if err != nil {
		return pc.encodeError(err)
	}
	if err = pc.rc.Flush(); err != nil {
		err = errors.Wrap(err, "Redis Encoder flush response")
	}
	return
}

func (pc *proxyConn) merge(msg *proto.Message) error {
	cmd, ok := msg.Request().(*Request)
	if !ok {
		return ErrBadAssert
	}
	if !msg.IsBatch() {
		return pc.encodeReply(cmd)
	}
	mtype, err := pc.getBatchMergeType(msg)
	if err != nil {
		return err
	}
	switch mtype {
	case MergeTypeJoin:
		return pc.mergeJoin(msg)
	case MergeTypeOk:
		return pc.mergeOk(msg)
	case MergeTypeCount:
		return pc.mergeCount(msg)
	case MergeTypeBasic:
		fallthrough
	default:
		panic("unreachable path")
	}
}

func (pc *proxyConn) encodeReply(req *Request) error {
	if req.rtype == reqTypeNotSupport {
		return robjErrNotSupport.encode(pc.rc.bw)
	}

	data := req.respObj.nth(0).data
	if bytes.Equal(data, cmdPingBytes) {
		return robjPong.encode(pc.rc.bw)
	}

	return req.reply.encode(pc.rc.bw)
}

func (pc *proxyConn) mergeOk(msg *proto.Message) (err error) {
	for _, sub := range msg.Subs() {
		if err = sub.Err(); err != nil {
			cmd := sub.Request().(*Request)
			pc.rc.bw.Write(respErrorBytes)
			pc.rc.bw.Write(cmd.reply.data)
			pc.rc.bw.Write(crlfBytes)
			return
		}
	}
	pc.rc.bw.Write(respStringBytes)
	pc.rc.bw.Write(okBytes)
	pc.rc.bw.Write(crlfBytes)
	return
}

func (pc *proxyConn) mergeCount(msg *proto.Message) (err error) {
	var sum = 0
	for _, sub := range msg.Subs() {
		if sub.Err() != nil {
			continue
		}
		subcmd, ok := sub.Request().(*Request)
		if !ok {
			return ErrBadAssert
		}
		ival, err := conv.Btoi(subcmd.reply.data)
		if err != nil {
			return ErrBadCount
		}
		sum += int(ival)
	}
	pc.rc.bw.Write(respIntBytes)
	pc.rc.bw.Write([]byte(strconv.Itoa(sum)))
	pc.rc.bw.Write(crlfBytes)
	return
}

func (pc *proxyConn) mergeJoin(msg *proto.Message) (err error) {
	subs := msg.Subs()
	pc.rc.bw.Write(respArrayBytes)
	if len(subs) == 0 {
		pc.rc.bw.Write(respNullBytes)
		return
	}
	pc.rc.bw.Write([]byte(strconv.Itoa(len(subs))))
	pc.rc.bw.Write(crlfBytes)
	for _, sub := range subs {
		subcmd, ok := sub.Request().(*Request)
		if !ok {
			err = pc.encodeError(ErrBadAssert)
			if err != nil {
				return
			}
			return ErrBadAssert
		}
		subcmd.reply.encode(pc.rc.bw)
	}
	return
}

func (pc *proxyConn) getBatchMergeType(msg *proto.Message) (mtype MergeType, err error) {
	cmd, ok := msg.Subs()[0].Request().(*Request)
	if !ok {
		err = ErrBadAssert
		return
	}
	mtype = cmd.mergeType
	return
}

func (pc *proxyConn) encodeError(err error) error {
	se := errors.Cause(err).Error()
	pc.rc.bw.Write(respErrorBytes)
	pc.rc.bw.Write([]byte(se))
	pc.rc.bw.Write(crlfBytes)
	return pc.rc.bw.Flush()
}
