package redis

import (
	"bytes"
	"fmt"
	"io"
	"strconv"
	"testing"

	"overlord/proto"

	"github.com/stretchr/testify/assert"
)

type mockCmd struct {
}

func (*mockCmd) CmdString() string {
	return ""
}

func (*mockCmd) Cmd() []byte {
	return []byte("")
}

func (*mockCmd) Key() []byte {
	return []byte{}
}

func (*mockCmd) Put() {
}

func TestNodeConnWriteBatchOk(t *testing.T) {
	nc := newNodeConn("baka", "127.0.0.1:12345", _createConn(nil))
	mb := proto.NewMsgBatch()
	msg := proto.GetMsg()
	req := getReq()
	req.mType = mergeTypeNo
	req.resp = &resp{
		rTp:  respArray,
		data: []byte("2"),
		array: []*resp{
			&resp{
				rTp:  respBulk,
				data: []byte("3\r\nGET"),
			},
			&resp{
				rTp:  respBulk,
				data: []byte("5\r\nabcde"),
			},
		},
		arrayn: 2,
	}
	msg.WithRequest(req)
	mb.AddMsg(msg)
	err := nc.WriteBatch(mb)
	assert.NoError(t, err)
}

func TestNodeConnWriteBadAssert(t *testing.T) {
	nc := newNodeConn("baka", "127.0.0.1:12345", _createConn(nil))
	mb := proto.NewMsgBatch()
	msg := proto.GetMsg()
	msg.WithRequest(&mockCmd{})
	mb.AddMsg(msg)

	err := nc.WriteBatch(mb)
	assert.Error(t, err)
	assert.Equal(t, ErrBadAssert, err)
}

func TestReadBatchOk(t *testing.T) {
	data := ":1\r\n"
	nc := newNodeConn("baka", "127.0.0.1:12345", _createConn([]byte(data)))
	mb := proto.NewMsgBatch()
	msg := proto.GetMsg()
	req := getReq()
	req.mType = mergeTypeNo
	req.reply = &resp{}
	msg.WithRequest(req)
	mb.AddMsg(msg)
	err := nc.ReadBatch(mb)
	assert.NoError(t, err)
}

func TestReadBatchWithBadAssert(t *testing.T) {
	nc := newNodeConn("baka", "127.0.0.1:12345", _createConn([]byte(":123\r\n")))
	mb := proto.NewMsgBatch()
	msg := proto.GetMsg()
	msg.WithRequest(&mockCmd{})
	mb.AddMsg(msg)
	err := nc.ReadBatch(mb)
	assert.Error(t, err)
	assert.Equal(t, ErrBadAssert, err)
}

func TestReadBatchWithNilError(t *testing.T) {
	nc := newNodeConn("baka", "127.0.0.1:12345", _createConn(nil))
	mb := proto.NewMsgBatch()
	msg := proto.GetMsg()
	req := getReq()
	req.mType = mergeTypeJoin
	req.reply = &resp{}
	req.resp = newresp(respArray, []byte("2"))
	req.resp.array = append(req.resp.array, newresp(respBulk, []byte("get")))
	req.resp.arrayn++
	msg.WithRequest(req)
	mb.AddMsg(msg)
	err := nc.ReadBatch(mb)
	assert.Error(t, err)
	assert.Equal(t, io.EOF, err)
}

func TestPingOk(t *testing.T) {
	nc := newNodeConn("baka", "127.0.0.1:12345", _createRepeatConn(pongBytes, 1))
	err := nc.Ping()
	assert.NoError(t, err)
}

func newRequest(cmd string, args ...string) *Request {
	respObj := &resp{}
	respObj.array = append(respObj.array, newresp(respBulk, []byte(fmt.Sprintf("%d\r\n%s", len(cmd), cmd))))
	respObj.arrayn++
	maxLen := len(args) + 1
	for i := 1; i < maxLen; i++ {
		data := args[i-1]
		line := fmt.Sprintf("%d\r\n%s", len(data), data)
		respObj.array = append(respObj.array, newresp(respBulk, []byte(line)))
		respObj.arrayn++
	}
	respObj.data = []byte(strconv.Itoa(len(args) + 1))
	return &Request{
		resp:  respObj,
		mType: getMergeType(respObj.array[0].data),
		reply: &resp{},
	}
}
func getMergeType(cmd []byte) mergeType {
	// fmt.Println("mtype :", strconv.Quote(string(cmd)))
	// TODO: impl with tire tree to search quickly
	if bytes.Equal(cmd, cmdMGetBytes) || bytes.Equal(cmd, cmdGetBytes) {
		return mergeTypeJoin
	}

	if bytes.Equal(cmd, cmdMSetBytes) {
		return mergeTypeOK
	}

	if bytes.Equal(cmd, cmdExistsBytes) || bytes.Equal(cmd, cmdDelBytes) {
		return mergeTypeCount
	}

	return mergeTypeNo
}

func newresp(rtype respType, data []byte) (robj *resp) {
	robj = &resp{}
	robj.rTp = rtype
	robj.data = data
	return
}

func newrespArray(resps []*resp) (robj *resp) {
	robj = &resp{}
	robj.rTp = respArray
	robj.data = []byte((strconv.Itoa(len(resps))))
	robj.array = resps
	robj.arrayn = len(resps)
	return
}
