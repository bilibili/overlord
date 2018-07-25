package redis

import (
	"bytes"
	"errors"

	"overlord/proto"
)

const (
	parityBit int = 1
)

// errors
var (
	ErrBadRequestSize = errors.New("wrong Mset arguments number")
)

func isEven(v int) bool {
	return v&parityBit == 0
}

func isComplex(cmd []byte) bool {
	return bytes.Equal(cmd, cmdMSetBytes) ||
		bytes.Equal(cmd, cmdMGetBytes) ||
		bytes.Equal(cmd, cmdDelBytes) ||
		bytes.Equal(cmd, cmdExistsBytes)
}

func newSubCmd(msg *proto.Message, robj *resp) error {
	if bytes.Equal(robj.nth(0).data, cmdMSetBytes) {
		return cmdMset(msg, robj)
	}
	return cmdByKeys(msg, robj)
}

func cmdMset(msg *proto.Message, robj *resp) error {
	if !isEven(robj.Len() - 1) {
		return ErrBadRequestSize
	}
	mid := robj.Len() / 2
	for i := 0; i < mid; i++ {
		req := msg.NextReq()
		if req == nil {
			cmdObj := respPool.Get().(*resp)
			cmdObj.rtype = respArray
			cmdObj.replace(0, newRESPBulk(cmdMSetBytes))
			cmdObj.replace(1, robj.nth(i*2+1))
			cmdObj.replace(2, robj.nth(i*2+2))
			cmdObj.data = cmdMSetLenBytes
			cmd := newRequestWithMergeType(cmdObj, MergeTypeOk)
			cmd.rtype = getReqType(cmdObj.nth(0).data)
			msg.WithRequest(cmd)
		} else {
			reqCmd := req.(*Request)
			reqCmd.mergeType = MergeTypeOk
			cmdObj := reqCmd.respObj
			cmdObj.rtype = respArray
			cmdObj.replace(0, newRESPBulk(cmdMSetBytes))
			cmdObj.replace(1, robj.nth(i*2+1))
			cmdObj.replace(2, robj.nth(i*2+2))
			cmdObj.data = cmdMSetLenBytes
			reqCmd.rtype = getReqType(cmdObj.nth(0).data)
		}
	}
	return nil
}

func cmdByKeys(msg *proto.Message, robj *resp) error {
	var cmdObj *resp
	for _, sub := range robj.slice()[1:] {
		req := msg.NextReq()
		if req == nil {
			if bytes.Equal(robj.nth(0).data, cmdMGetBytes) {
				cmdObj = newRESPArray([]*resp{newRESPBulk(cmdGetBytes), sub})
			} else {
				cmdObj = newRESPArray([]*resp{robj.nth(0), sub})
			}
			cmd := newRequest(cmdObj)
			msg.WithRequest(cmd)
		} else {
			reqCmd := req.(*Request)
			cmdObj := reqCmd.respObj
			if bytes.Equal(robj.nth(0).data, cmdMGetBytes) {
				cmdObj.setArray([]*resp{newRESPBulk(cmdGetBytes), sub})
			} else {
				cmdObj.setArray([]*resp{robj.nth(0), sub})
			}
			reqCmd.rtype = getReqType(cmdObj.nth(0).data)
			reqCmd.mergeType = getMergeType(cmdObj.nth(0).data)
		}
	}
	return nil
}
