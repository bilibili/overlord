package redis

import (
	"bytes"
	errs "errors"
	"sync"
)

var (
	emptyBytes = []byte("")
	crlfBytes  = []byte("\r\n")

	arrayLenTwo   = []byte("2")
	arrayLenThree = []byte("3")

	cmdPingBytes   = []byte("4\r\nPING")
	cmdMSetBytes   = []byte("4\r\nMSET")
	cmdMGetBytes   = []byte("4\r\nMGET")
	cmdGetBytes    = []byte("3\r\nGET")
	cmdDelBytes    = []byte("3\r\nDEL")
	cmdExistsBytes = []byte("6\r\nEXISTS")

	respNullBytes = []byte("-1\r\n")
	okBytes       = []byte("OK")

	notSupportCmdBytes = []byte("" +
		"6\r\nMSETNX" +
		"5\r\nBLPOP" +
		"5\r\nBRPOP" +
		"10\r\nBRPOPLPUSH" +
		"4\r\nKEYS" +
		"7\r\nMIGRATE" +
		"4\r\nMOVE" +
		"6\r\nOBJECT" +
		"9\r\nRANDOMKEY" +
		"6\r\nRENAME" +
		"8\r\nRENAMENX" +
		"4\r\nSCAN" +
		"4\r\nWAIT" +
		"5\r\nBITOP" +
		"4\r\nEVAL" +
		"7\r\nEVALSHA" +
		"4\r\nAUTH" +
		"4\r\nECHO" +
		"4\r\nINFO" +
		"5\r\nPROXY" +
		"7\r\nSLOWLOG" +
		"4\r\nQUIT" +
		"6\r\nSELECT" +
		"4\r\nTIME" +
		"6\r\nCONFIG" +
		"8\r\nCOMMANDS")
)

// errors
var (
	ErrBadAssert  = errs.New("bad assert for redis")
	ErrBadCount   = errs.New("bad count number")
	ErrBadRequest = errs.New("bad request")
)

// mergeType is used to decript the merge operation.
type mergeType = uint8

// merge types
const (
	mergeTypeNo mergeType = iota
	mergeTypeCount
	mergeTypeOK
	mergeTypeJoin
)

// Request is the type of a complete redis command
type Request struct {
	resp  *resp
	reply *resp
	mType mergeType
}

var reqPool = &sync.Pool{
	New: func() interface{} {
		return newReq()
	},
}

// getReq get the msg from pool
func getReq() *Request {
	return reqPool.Get().(*Request)
}

func newReq() *Request {
	r := &Request{}
	r.resp = &resp{}
	r.reply = &resp{}
	return r
}

// CmdString get the cmd
func (r *Request) CmdString() string {
	return string(r.Cmd())
}

// Cmd get the cmd
func (r *Request) Cmd() []byte {
	if r.resp.arrayn < 1 {
		return emptyBytes
	}
	cmd := r.resp.array[0]
	var pos int
	if cmd.rTp == respBulk {
		pos = bytes.Index(cmd.data, crlfBytes) + 2
	}
	return cmd.data[pos:]
}

// Key impl the proto.protoRequest and get the Key of redis
func (r *Request) Key() []byte {
	if r.resp.arrayn < 1 {
		return emptyBytes
	}
	if r.resp.arrayn == 1 {
		return r.resp.array[0].data
	}
	k := r.resp.array[1]
	var pos int
	if k.rTp == respBulk {
		pos = bytes.Index(k.data, crlfBytes) + 2
	}
	return k.data[pos:]
}

// Put the resource back to pool
func (r *Request) Put() {
	r.resp.reset()
	r.reply.reset()
	r.mType = mergeTypeNo
	reqPool.Put(r)
}

// notSupport command not support
func (r *Request) notSupport() bool {
	if r.resp.arrayn < 1 {
		return false
	}
	return bytes.Index(notSupportCmdBytes, r.resp.array[0].data) > -1
}
