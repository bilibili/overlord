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

// func subCmdMset(robj *resp) ([]*Request, error) {
// 	if !isEven(robj.Len() - 1) {
// 		return nil, ErrBadRequestSize
// 	}

// 	mid := robj.Len() / 2
// 	cmds := make([]*Request, mid)
// 	for i := 0; i < mid; i++ {
// 		cmdObj := respPool.Get().(*resp)
// 		cmdObj.rtype = respArray
// 		cmdObj.data = nil
// 		cmdObj.replace(0, robjMSet)
// 		cmdObj.replace(1, robj.nth(i*2+1))
// 		cmdObj.replace(2, robj.nth(i*2+2))
// 		cmdObj.data = cmdMSetLenBytes
// 		cmds[i] = newRequestWithMergeType(cmdObj, MergeTypeOk)
// 	}
// 	return cmds, nil
// }

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
			cmdObj.data = nil
			cmdObj.replace(0, robjMSet)
			cmdObj.replace(1, robj.nth(i*2+1))
			cmdObj.replace(2, robj.nth(i*2+2))
			cmdObj.data = cmdMSetLenBytes
			cmd := newRequestWithMergeType(cmdObj, MergeTypeOk)
			msg.WithRequest(cmd)
		} else {
			reqCmd := req.(*Request)
			reqCmd.mergeType = MergeTypeOk
			cmdObj := reqCmd.respObj
			cmdObj.rtype = respArray
			cmdObj.data = nil
			cmdObj.replace(0, robjMSet)
			cmdObj.replace(1, robj.nth(i*2+1))
			cmdObj.replace(2, robj.nth(i*2+2))
			cmdObj.data = cmdMSetLenBytes
		}
	}
	return nil
}

// func subCmdByKeys(robj *resp) ([]*Request, error) {
// 	cmds := make([]*Request, robj.Len()-1)
// 	for i, sub := range robj.slice()[1:] {
// 		var cmdObj *resp
// 		if bytes.Equal(robj.nth(0).data, cmdMGetBytes) {
// 			cmdObj = newRESPArray([]*resp{robjGet, sub})
// 		} else {
// 			cmdObj = newRESPArray([]*resp{robj.nth(0), sub})
// 		}
// 		cmds[i] = newRequest(cmdObj)
// 	}
// 	return cmds, nil
// }

func cmdByKeys(msg *proto.Message, robj *resp) error {
	var cmdObj *resp
	for _, sub := range robj.slice()[1:] {
		req := msg.NextReq()
		if req == nil {
			if bytes.Equal(robj.nth(0).data, cmdMGetBytes) {
				cmdObj = newRESPArray([]*resp{robjGet, sub})
			} else {
				cmdObj = newRESPArray([]*resp{robj.nth(0), sub})
			}
			cmd := newRequest(cmdObj)
			msg.WithRequest(cmd)
		} else {
			reqCmd := req.(*Request)
			cmdObj := reqCmd.respObj
			if bytes.Equal(robj.nth(0).data, cmdMGetBytes) {
				cmdObj.setArray([]*resp{robjGet, sub})
			} else {
				cmdObj.setArray([]*resp{robj.nth(0), sub})
			}
			reqCmd.mergeType = getMergeType(cmdObj.nth(0).data)
		}
	}
	return nil
}
