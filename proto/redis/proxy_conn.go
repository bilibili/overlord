package redis

import (
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
	rs, err := pc.rc.decodeMax(msgs)
	if err != nil {
		return msgs, err
	}
	for i, r := range rs {
		msgs[i].Type = proto.CacheTypeRedis
		msgs[i].MarkStart()

		err = pc.decodeToMsg(r, msgs[i])
		if err != nil {
			msgs[i].Reset()
			return nil, err
		}
	}
	return msgs[:len(rs)], nil
}

func (pc *proxyConn) decodeToMsg(robj *resp, msg *proto.Message) (err error) {
	if isComplex(robj.nth(0).data) {
		cmds, inerr := newSubCmd(robj)
		if inerr != nil {
			err = inerr
			return
		}
		for _, cmd := range cmds {
			pc.withReq(msg, cmd)
		}
	} else {
		pc.withReq(msg, newCommand(robj))
	}
	return
}

func (pc *proxyConn) withReq(m *proto.Message, cmd *Command) {
	req := m.NextReq()
	if req == nil {
		m.WithRequest(cmd)
	} else {
		reqCmd := req.(*Command)
		reqCmd.respObj = cmd.respObj
		reqCmd.mergeType = cmd.mergeType
		reqCmd.reply = cmd.reply
	}
}

func (pc *proxyConn) Encode(msg *proto.Message) (err error) {
	if err := msg.Err(); err != nil {
		return pc.encodeError(err)
	}
	err = pc.merge(msg)
	if err != nil {
		return pc.encodeError(err)
	}
	for _, item := range msg.SubResps() {
		_ = pc.rc.bw.Write(item)
	}
	if err = pc.rc.Flush(); err != nil {
		err = errors.Wrap(err, "Redis Encoder flush response")
	}
	return
}

func (pc *proxyConn) merge(msg *proto.Message) error {
	cmd, ok := msg.Request().(*Command)
	if !ok {
		return ErrBadAssert
	}
	if !msg.IsBatch() {
		return cmd.reply.encode(msg)
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

func (pc *proxyConn) mergeOk(msg *proto.Message) (err error) {
	for i, sub := range msg.Subs() {
		if err = sub.Err(); err != nil {
			cmd := sub.Request().(*Command)
			msg.Write(respErrorBytes)
			msg.Write(cmd.reply.data)
			msg.Write(crlfBytes)
			return
		}
		if i == len(msg.Subs())-1 {
			msg.Write(respStringBytes)
			msg.Write(okBytes)
			msg.Write(crlfBytes)
		}
	}
	return
}

func (pc *proxyConn) mergeCount(msg *proto.Message) (err error) {
	var sum = 0
	for _, sub := range msg.Subs() {
		if sub.Err() != nil {
			continue
		}
		subcmd, ok := sub.Request().(*Command)
		if !ok {
			return ErrBadAssert
		}
		ival, err := conv.Btoi(subcmd.reply.data)
		if err != nil {
			return ErrBadCount
		}
		sum += int(ival)
	}
	msg.Write(respIntBytes)
	msg.Write([]byte(strconv.Itoa(sum)))
	msg.Write(crlfBytes)
	return
}

func (pc *proxyConn) mergeJoin(msg *proto.Message) (err error) {
	// TODO (LINTANGHUI):reuse reply
	subs := msg.Subs()
	msg.Write(respArrayBytes)
	if len(subs) == 0 {
		msg.Write(respNullBytes)
		return
	}
	msg.Write([]byte(strconv.Itoa(len(subs))))
	msg.Write(crlfBytes)
	for _, sub := range subs {
		subcmd, ok := sub.Request().(*Command)
		if !ok {
			err = pc.encodeError(ErrBadAssert)
			if err != nil {
				return
			}
			return ErrBadAssert
		}
		subcmd.reply.encode(msg)
	}
	return
}

func (pc *proxyConn) getBatchMergeType(msg *proto.Message) (mtype MergeType, err error) {
	cmd, ok := msg.Subs()[0].Request().(*Command)
	if !ok {
		err = ErrBadAssert
		return
	}
	mtype = cmd.mergeType
	return
}

func (pc *proxyConn) encodeError(err error) error {
	se := errors.Cause(err).Error()
	robj := newRESPPlain(respError, []byte(se))
	return robj.encode(pc.rc.bw)
}
