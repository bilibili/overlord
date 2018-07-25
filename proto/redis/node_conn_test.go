package redis

import (
	"testing"

	"overlord/lib/bufio"
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
	req.mType = mergeTypeNo
	req.reply = &resp{}
	msg.WithRequest(req)
	mb.AddMsg(msg)
	err := nc.ReadBatch(mb)
	assert.Error(t, err)
	assert.Equal(t, bufio.ErrBufferFull, err)
}

func TestPingOk(t *testing.T) {
	nc := newNodeConn("baka", "127.0.0.1:12345", _createRepeatConn(pongBytes, 1))
	err := nc.Ping()
	assert.NoError(t, err)
}
