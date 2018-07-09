package redis

import (
	"errors"
	"strings"
	// "crc"
)

var (
	crlfBytes  = []byte{'\r', '\n'}
	lfByte     = byte('\n')
	movedBytes = []byte("MOVED")
	askBytes   = []byte("ASK")
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
}

// NewCommand will create new command by given args
// example:
//     NewCommand("GET", "mykey")
//     NewCommand("MGET", "mykey", "yourkey")
// func NewCommand(cmd string, args ...string) *Command {
// 	respObj := newRespArrayWithCapcity(len(args) + 1)
// 	respObj.replace(0, newRespBulk([]byte(cmd)))
// 	maxLen := len(args) + 1
// 	for i := 1; i < maxLen; i++ {
// 		respObj.replace(i, newRespBulk([]byte(args[i-1])))
// 	}
// 	return newCommand(respObj)
// }

func newCommand(robj *resp) *Command {
	r := &Command{respObj: robj}
	r.mergeType = getMergeType(robj.nth(0).data)
	return r
}

// // Slot will caculate the redis crc and return the slot value
// func (rr *Command) Slot() int {
// 	keyData := rr.respObj.nth(1).data

// 	// support HashTag
// 	idx := bytes.IndexByte(keyData, '{')
// 	if idx != -1 {
// 		eidx := bytes.IndexByte(keyData, '}')
// 		if eidx > idx {
// 			// matched
// 			keyData = keyData[idx+1 : eidx]
// 		}
// 	}
// 	// TODO: crc16
// 	// crcVal := crc.Crc16(string(keyData))
// 	// return int(crcVal) & SlotShiled
// 	return 0
// }

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
	return c.respObj.nth(1).data
}

// Put the resource back to pool
func (c *Command) Put() {
}

// Resp return the response bytes of resp
func (c *Command) Resp() []byte {
	return []byte{}
}

// // IsBatch impl the proto.protoRequest and check if the command is batchable
// func (rr *Command) IsBatch() bool {
// 	return rr.batchStep != defaultBatchStep
// }

// // // Batch impl the proto.protoRequest and split the command into divided part.
// // func (rr *Command) Batch() ([]proto.Request, *proto.Response) {
// // 	if rr.batchStep == defaultBatchStep {
// // 		// batch but only split one
// // 		return rr.batchOne()
// // 	}

// // 	return rr.batchByStep(rr.batchStep)
// // }

// // func (rr *Command) batchOne() ([]proto.Request, *proto.Response) {
// // 	reqs := []proto.Request{
// // 		proto.Request{
// // 			Type: proto.CacheTypeRedis,
// // 		},
// // 	}
// // 	reqs[0].WithProto(rr)
// // 	response := &proto.Response{
// // 		Type: proto.CacheTypeRedis,
// // 	}
// // 	return reqs, response
// // }

// // func (rr *Command) batchByStep(step int) ([]proto.Request, *proto.Response) {
// // 	// NEEDTEST(wayslog): we assume that the request is full.

// // 	// trim cmd
// // 	cmd := rr.Cmd()
// // 	mergeType := getMergeType(cmd)

// // 	slice := rr.respObj.slice()[1:]

// // 	items := (rr.respObj.Len() - 1) / step
// // 	resps := make([]proto.Request, items)

// // 	batchCmd := getBatchCmd(cmd)

// // 	bcmdResp := newRespString(batchCmd)
// // 	bcmdType := getCmdType(batchCmd)

// // 	for i := 0; i < items; i++ {
// // 		// keyPos := i*step
// // 		// argsBegin := i*step+1
// // 		// argsEnd := i*step + step -1
// // 		r := newRespArrayWithCapcity(step + 1)
// // 		r.replace(0, bcmdResp)
// // 		r.replaceAll(1, slice[i*step:(i+1)*step])

// // 		req := proto.Request{Type: proto.CacheTypeRedis}
// // 		req.WithProto(&Command{
// // 			respObj:   r,
// // 			cmdType:   bcmdType,
// // 			batchStep: defaultBatchStep,
// // 		})
// // 		resps[i] = req
// // 	}

// // 	response := &proto.Response{
// // 		Type: proto.CacheTypeRedis,
// // 	}
// // 	response.WithProto(newRResponse(mergeType, nil))
// // 	return resps, response
// // }

// // RResponse is the redis response protocol type.
// type RResponse struct {
// 	respObj *resp

// 	mergeType MergeType
// }

// func newRResponse(mtype MergeType, robj *resp) *RResponse {
// 	return &RResponse{
// 		mergeType: mtype,
// 		respObj:   robj,
// 	}
// }

// // Merge impl the proto.Merge interface
// func (rr *RResponse) Merge(subs []proto.Request) {
// 	switch rr.mergeType {
// 	case MergeTypeBasic:
// 		srr, ok := subs[0].Resp.Proto().(*RResponse)
// 		if !ok {
// 			// TOOD(wayslog): log it
// 			return
// 		}
// 		rr.respObj = srr.respObj
// 	case MergeTypeJoin:
// 		rr.mergeJoin(subs)
// 	case MergeTypeCount:
// 		rr.mergeCount(subs)
// 	case MergeTypeOk:
// 		rr.mergeOk(subs)
// 	}
// }

// func (rr *RResponse) mergeJoin(subs []proto.Request) {
// 	if rr.respObj == nil {
// 		rr.respObj = newRespArrayWithCapcity(len(subs))
// 	}
// 	if rr.respObj.isNull() {
// 		rr.respObj.array = make([]*resp, len(subs))
// 	}
// 	for idx, sub := range subs {
// 		srr, ok := sub.Resp.Proto().(*RResponse)
// 		if !ok {
// 			// TODO(wayslog): log it
// 			continue
// 		}
// 		rr.respObj.replace(idx, srr.respObj)
// 	}
// }

// // IsRedirect check if response type is Redis Error
// // and payload was prefix with "ASK" && "MOVED"
// func (rr *RResponse) IsRedirect() bool {
// 	if rr.respObj.rtype != respError {
// 		return false
// 	}
// 	if rr.respObj.data == nil {
// 		return false
// 	}

// 	return bytes.HasPrefix(rr.respObj.data, movedBytes) ||
// 		bytes.HasPrefix(rr.respObj.data, askBytes)
// }

// // RedirectTriple will check and send back by is
// // first return variable which was called as redirectType maybe return ASK or MOVED
// // second is the slot of redirect
// // third is the redirect addr
// // last is the error when parse the redirect body
// func (rr *RResponse) RedirectTriple() (redirect string, slot int, addr string, err error) {
// 	fields := strings.Fields(string(rr.respObj.data))
// 	if len(fields) != 3 {
// 		err = ErrRedirectBadFormat
// 		return
// 	}
// 	redirect = fields[0]
// 	addr = fields[2]
// 	ival, parseErr := strconv.Atoi(fields[1])

// 	slot = ival
// 	err = parseErr
// 	return
// }

// func (rr *RResponse) mergeCount(subs []proto.Request) {
// 	count := 0
// 	for _, sub := range subs {
// 		if err := sub.Resp.Err(); err != nil {
// 			// TODO(wayslog): log it
// 			continue
// 		}
// 		ssr, ok := sub.Resp.Proto().(*RResponse)
// 		if !ok {
// 			continue
// 		}
// 		ival, err := strconv.Atoi(string(ssr.respObj.data))
// 		if err != nil {
// 			continue
// 		}
// 		count += ival
// 	}
// 	rr.respObj = newRespInt(count)
// }

// func (rr *RResponse) mergeOk(subs []proto.Request) {
// 	for _, sub := range subs {
// 		if err := sub.Resp.Err(); err != nil {
// 			// TODO(wayslog): set as bad response
// 			return
// 		}
// 	}
// 	rr.respObj = newRespString("OK")
// }
