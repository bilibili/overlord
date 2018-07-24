package redis

import (
	"errors"
	"fmt"
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
	cmdPingBytes    = []byte("4\r\nPING")
	cmdMSetLenBytes = []byte("3")
	cmdMSetBytes    = []byte("4\r\nMSET")
	cmdMGetBytes    = []byte("4\r\nMGET")
	cmdGetBytes     = []byte("3\r\nGET")
	cmdDelBytes     = []byte("3\r\nDEL")
	cmdExistsBytes  = []byte("6\r\nEXISTS")
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

// Request is the type of a complete redis command
type Request struct {
	respObj   *resp
	mergeType MergeType
	reply     *resp
	rtype     reqType
}

// NewRequest will create new command by given args
// example:
//     NewRequest("GET", "mykey")
//     NewRequest("CLUSTER", "NODES")
func NewRequest(cmd string, args ...string) *Request {
	respObj := respPool.Get().(*resp)
	respObj.next().setBulk([]byte(cmd))
	// respObj := newRESPArrayWithCapcity(len(args) + 1)
	// respObj.replace(0, newRESPBulk([]byte(cmd)))
	maxLen := len(args) + 1
	for i := 1; i < maxLen; i++ {
		data := args[i-1]
		line := fmt.Sprintf("%d\r\n%s", len(data), data)
		respObj.next().setBulk([]byte(line))
	}
	respObj.data = []byte(strconv.Itoa(len(args) + 1))
	return newRequest(respObj)
}

func newRequest(robj *resp) *Request {
	r := &Request{respObj: robj}
	r.mergeType = getMergeType(robj.nth(0).data)
	r.rtype = getReqType(robj.nth(0).data)
	r.reply = &resp{}
	return r
}

func (c *Request) setRESP(robj *resp) {
	c.respObj = robj
	c.mergeType = getMergeType(robj.nth(0).data)
	c.rtype = getReqType(robj.nth(0).data)
	c.reply = &resp{}
}

func newRequestWithMergeType(robj *resp, mtype MergeType) *Request {
	return &Request{respObj: robj, mergeType: mtype}
}

// CmdString get the cmd
func (c *Request) CmdString() string {
	return strings.ToUpper(string(c.respObj.nth(0).data))
}

// Cmd get the cmd
func (c *Request) Cmd() []byte {
	return c.respObj.nth(0).data
}

// Key impl the proto.protoRequest and get the Key of redis
func (c *Request) Key() []byte {
	if len(c.respObj.array) == 1 {
		return c.respObj.nth(0).data
	}
	var data = c.respObj.nth(1).data
	var pos int
	if c.respObj.nth(1).rtype == respBulk {
		pos = bytes.Index(data, crlfBytes) + 2
	}
	// pos is never lower than 0
	return data[pos:]
}

// Put the resource back to pool
func (c *Request) Put() {
}

// IsRedirect check if response type is Redis Error
// and payload was prefix with "ASK" && "MOVED"
func (c *Request) IsRedirect() bool {
	if c.reply.rtype != respError {
		return false
	}
	if c.reply.data == nil {
		return false
	}

	return bytes.HasPrefix(c.reply.data, movedBytes) ||
		bytes.HasPrefix(c.reply.data, askBytes)
}

// RedirectTriple will check and send back by is
// first return variable which was called as redirectType maybe return ASK or MOVED
// second is the slot of redirect
// third is the redirect addr
// last is the error when parse the redirect body
func (c *Request) RedirectTriple() (redirect string, slot int, addr string, err error) {
	fields := strings.Fields(string(c.reply.data))
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
