package redis

import (
	stderrs "errors"
	"overlord/lib/conv"
	libnet "overlord/lib/net"
	"overlord/proto"

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
		rc: newRespConn(conn),
	}
	return r
}

func (pc *proxyConn) Decode(msgs []*proto.Message) ([]*proto.Message, error) {
	var (
		rs  []*resp
		err error
	)
	rs, err = pc.rc.decodeMax(len(msgs))
	if err != nil {
		return msgs, err
	}
	for i, r := range rs {
		msgs[i].Type = proto.CacheTypeRedis
		msgs[i].MarkStart()
		err = pc.decodeToMsg(r, msgs[i])
		if err != nil {
			msgs[i].Reset()
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
			msg.WithRequest(cmd)
		}
	} else {
		msg.WithRequest(newCommand(robj))
	}
	return
}

func (pc *proxyConn) Encode(msg *proto.Message) (err error) {
	if err = pc.encode(msg); err != nil {
		err = errors.Wrap(err, "Redis Encoder encode")
		return
	}

	if err = pc.rc.Flush(); err != nil {
		err = errors.Wrap(err, "Redis Encoder flush response")
	}
	return
}

func (pc *proxyConn) encode(msg *proto.Message) error {
	if err := msg.Err(); err != nil {
		return pc.encodeError(err)
	}
	reply, err := pc.merge(msg)
	if err != nil {
		return pc.encodeError(err)
	}
	return pc.rc.encode(reply)
}

func (pc *proxyConn) merge(msg *proto.Message) (*resp, error) {
	cmd, ok := msg.Request().(*Command)
	if !ok {
		return nil, ErrBadAssert
	}

	if !msg.IsBatch() {
		return cmd.reply, nil
	}

	mtype, err := pc.getBatchMergeType(msg)
	if err != nil {
		return nil, err
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

func (pc *proxyConn) mergeOk(msg *proto.Message) (*resp, error) {
	return newRespString(okBytes), nil
}

func (pc *proxyConn) mergeCount(msg *proto.Message) (reply *resp, err error) {
	var sum = 0
	for _, sub := range msg.Subs() {
		if sub.Err() != nil {
			continue
		}
		subcmd, ok := sub.Request().(*Command)
		if !ok {
			return nil, ErrBadAssert
		}
		ival, err := conv.Btoi(subcmd.reply.data)
		if err != nil {
			return nil, ErrBadCount
		}
		sum += int(ival)
	}
	reply = newRespInt(sum)
	return
}

func (pc *proxyConn) mergeJoin(msg *proto.Message) (reply *resp, err error) {
	reply = newRespArrayWithCapcity(len(msg.Subs()))
	for i, sub := range msg.Subs() {
		subcmd, ok := sub.Request().(*Command)
		if !ok {
			err = pc.encodeError(ErrBadAssert)
			if err != nil {
				return
			}
			continue
		}
		reply.replace(i, subcmd.reply)
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
	robj := newRespPlain(respError, []byte(se))
	return pc.rc.encode(robj)
}
