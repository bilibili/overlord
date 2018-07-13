package redis

import (
	"bytes"
	"errors"
)

const (
	parityBit int = 1
)

// errors
var (
	ErrBadCommandSize = errors.New("wrong Mset arguments number")
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

func newSubCmd(robj *resp) ([]*Command, error) {
	if bytes.Equal(robj.nth(0).data, cmdMSetBytes) {
		return subCmdMset(robj)
	}
	return subCmdByKeys(robj)
}

func subCmdMset(robj *resp) ([]*Command, error) {
	if !isEven(robj.Len() - 1) {
		return nil, ErrBadCommandSize
	}

	mid := robj.Len() / 2
	cmds := make([]*Command, mid)
	for i := 0; i < mid; i++ {
		cmdObj := newRespArrayWithCapcity(3)
		cmdObj.replace(0, robjGet)
		cmdObj.replace(1, robj.nth(i*2+1))
		cmdObj.replace(2, robj.nth(i*2+2))
		cmds[i] = newCommandWithMergeType(cmdObj, MergeTypeOk)
	}
	return cmds, nil
}

func subCmdByKeys(robj *resp) ([]*Command, error) {
	cmds := make([]*Command, robj.Len()-1)
	for i, sub := range robj.slice()[1:] {
		cmdObj := newRespArray([]*resp{robj.nth(0), sub})
		cmds[i] = newCommand(cmdObj)
	}
	return cmds, nil
}
