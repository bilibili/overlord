package redis

import "errors"
import "bytes"

const (
	parityBit int = 0x0000000000000001
)

// errors
var (
	ErrBadCommandSize = errors.New("wrong command format")
)

var (
	robjGet = newRespBulk([]byte("get"))

	cmdMSetBytes   = []byte("MSET")
	cmdMGetBytes   = []byte("MGET")
	cmdDelBytes    = []byte("DEL")
	cmdExistsBytes = []byte("EXITS")
)

func isEven(v int) bool {
	return v&parityBit == 0
}

func isComplex(cmd []byte) bool {
	return bytes.Equal(cmd, cmdMSetBytes) || bytes.Equal(cmd, cmdMGetBytes) || bytes.Equal(cmd, cmdDelBytes) || bytes.Equal(cmd, cmdExistsBytes)
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
		cmds[i] = newCommand(cmdObj)
	}
	return cmds, nil
}

func subCmdByKeys(robj *resp) ([]*Command, error) {
	cmds := make([]*Command, robj.Len()-1)
	for i, sub := range robj.slice()[1:] {
		cmdObj := newRespArrayWithCapcity(2)
		cmdObj.replace(0, robj.nth(0))
		cmdObj.replace(1, sub)
		cmds[i] = newCommand(cmdObj)
	}
	return cmds, nil
}
