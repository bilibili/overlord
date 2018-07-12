package redis

import (
	"errors"
	"strconv"
	"strings"
	// "crc"
	"bytes"
)

var (
	crlfBytes  = []byte{'\r', '\n'}
	lfByte     = byte('\n')
	movedBytes = []byte("MOVED")
	askBytes   = []byte("ASK")
)

var (
	robjGet = newRespBulk([]byte("get"))

	cmdMSetBytes   = []byte("MSET")
	cmdMGetBytes   = []byte("MGET")
	cmdDelBytes    = []byte("DEL")
	cmdExistsBytes = []byte("EXITS")
)

// errors
var (
	ErrProxyFail         = errors.New("fail to send proxy")
	ErrRequestBadFormat  = errors.New("redis must be a RESP array")
	ErrRedirectBadFormat = errors.New("bad format of MOVED or ASK")
)

// const values
const (
	SlotCount  = 16384
	SlotShiled = 0x3fff
)

// Command is the type of a complete redis command
type Command struct {
	respObj   *resp
	mergeType MergeType
	reply     *resp
}

// NewCommand will create new command by given args
// example:
//     NewCommand("GET", "mykey")
//     NewCommand("MGET", "mykey", "yourkey")
func NewCommand(cmd string, args ...string) *Command {
	respObj := newRespArrayWithCapcity(len(args) + 1)
	respObj.replace(0, newRespBulk([]byte(cmd)))
	maxLen := len(args) + 1
	for i := 1; i < maxLen; i++ {
		respObj.replace(i, newRespBulk([]byte(args[i-1])))
	}
	return newCommand(respObj)
}

func newCommand(robj *resp) *Command {
	r := &Command{respObj: robj}
	r.mergeType = getMergeType(robj.nth(0).data)
	return r
}

func newCommandWithMergeType(robj *resp, mtype MergeType) *Command {
	return &Command{respObj: robj, mergeType: mtype}
}

// Slot will caculate the redis crc and return the slot value
func (c *Command) Slot() int {
	// TODO:CRC16
	// keyData := rr.respObj.nth(1).data

	// // support HashTag
	// idx := bytes.IndexByte(keyData, '{')
	// if idx != -1 {
	// 	eidx := bytes.IndexByte(keyData, '}')
	// 	if eidx > idx {
	// 		// matched
	// 		keyData = keyData[idx+1 : eidx]
	// 	}
	// }
	// crcVal := crc.Crc16(string(keyData))
	// return int(crcVal) & SlotShiled
	return 0
}

// CmdString get the cmd
func (c *Command) CmdString() string {
	return strings.ToUpper(c.respObj.nth(0).String())
}

// Cmd get the cmd
func (c *Command) Cmd() []byte {
	return c.respObj.nth(0).data
}

// Key impl the proto.protoRequest and get the Key of redis
func (c *Command) Key() []byte {
	var data = c.respObj.nth(1).data
	var pos int
	if c.respObj.rtype == respBulk {
		pos = bytes.Index(data, crlfBytes) + 2
	}
	// pos is never empty
	return data[pos:]
}

// Put the resource back to pool
func (c *Command) Put() {
}

// IsRedirect check if response type is Redis Error
// and payload was prefix with "ASK" && "MOVED"
func (c *Command) IsRedirect() bool {
	if c.respObj.rtype != respError {
		return false
	}
	if c.respObj.data == nil {
		return false
	}

	return bytes.HasPrefix(c.respObj.data, movedBytes) ||
		bytes.HasPrefix(c.respObj.data, askBytes)
}

// RedirectTriple will check and send back by is
// first return variable which was called as redirectType maybe return ASK or MOVED
// second is the slot of redirect
// third is the redirect addr
// last is the error when parse the redirect body
func (c *Command) RedirectTriple() (redirect string, slot int, addr string, err error) {
	fields := strings.Fields(string(c.respObj.data))
	if len(fields) != 3 {
		err = ErrRedirectBadFormat
		return
	}
	redirect = fields[0]
	addr = fields[2]
	ival, parseErr := strconv.Atoi(fields[1])

	slot = ival
	err = parseErr
	return
}
