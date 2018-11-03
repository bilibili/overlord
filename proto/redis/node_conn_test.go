package redis

import (
	"bytes"
	"fmt"
	"io"
	"strconv"
	"testing"
	"time"

	"overlord/lib/bufio"
	"overlord/proto"

	"github.com/pkg/errors"
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

func TestNodeConnNewNodeConn(t *testing.T) {
	nc := NewNodeConn("test", "127.0.0.1:12345", time.Second, time.Second, time.Second)
	assert.NotNil(t, nc)
	rnc := nc.(*nodeConn)
	assert.NotNil(t, rnc.Bw())
	assert.NoError(t, rnc.Close())
}

func TestNodeConnClose(t *testing.T) {
	nc := newNodeConn("baka", "127.0.0.1:12345", _createConn(nil))

	err := nc.Close()
	assert.NoError(t, err)
	err = nc.Close()
	assert.NoError(t, err)
}

func TestNodeConnWriteOk(t *testing.T) {
	nc := newNodeConn("baka", "127.0.0.1:12345", _createConn(nil))
	msg := proto.NewMessage()
	req := newRequest("GET", "AA")
	msg.WithRequest(req)
	nc.Write(msg)
	err := nc.Flush()
	assert.NoError(t, err)

	msg = proto.NewMessage()
	req = newRequest("unsupport")
	msg.WithRequest(req)
	rnc := nc.(*nodeConn)
	assert.NoError(t, rnc.Close())
	assert.True(t, rnc.Closed())

	err = nc.Write(msg)
	assert.Error(t, err)
	err = nc.Flush()
	assert.Error(t, err)
}

func TestNodeConnWriteBadAssert(t *testing.T) {
	nc := newNodeConn("baka", "127.0.0.1:12345", _createConn(nil))
	msg := proto.NewMessage()
	msg.WithRequest(&mockCmd{})

	err := nc.Write(msg)
	assert.Error(t, err)
	assert.Equal(t, ErrBadAssert, errors.Cause(err))
}

func TestNodeConnWriteHasErr(t *testing.T) {
	nc := newNodeConn("baka", "127.0.0.1:12345", _createConn(nil))
	ncc := nc.(*nodeConn)
	ec := _createConn(nil)
	ec.Conn.(*mockConn).err = errors.New("write error")
	ncc.bw = bufio.NewWriter(ec)
	ncc.bw.Write([]byte("err"))
	ncc.bw.Flush() // action err

	msg := proto.NewMessage()
	req := newRequest("GET", "AA")
	msg.WithRequest(req)

	nc.Write(msg)
	err := nc.Flush()
	assert.Error(t, err)
	assert.EqualError(t, err, "write error")
}

func TestReadOk(t *testing.T) {
	data := ":1\r\n"
	nc := newNodeConn("baka", "127.0.0.1:12345", _createConn([]byte(data)))
	msg := proto.NewMessage()
	req := newRequest("GET", "a")
	msg.WithRequest(req)
	err := nc.Read(msg)
	assert.NoError(t, err)
}

func TestReadWithBadAssert(t *testing.T) {
	nc := newNodeConn("baka", "127.0.0.1:12345", _createConn([]byte(":123\r\n")))
	msg := proto.NewMessage()
	msg.WithRequest(&mockCmd{})
	err := nc.Read(msg)
	assert.Error(t, err)
	assert.Equal(t, ErrBadAssert, errors.Cause(err))
}

func TestReadHasErr(t *testing.T) {
	nc := newNodeConn("baka", "127.0.0.1:12345", _createConn([]byte(":123\r\n")))
	ncc := nc.(*nodeConn)
	ec := _createConn(nil)
	ec.Conn.(*mockConn).err = errors.New("read error")
	ncc.br = bufio.NewReader(ec, bufio.Get(128))
	ncc.br.Read() // action err

	msg := proto.NewMessage()
	req := newRequest("GET", "a")
	msg.WithRequest(req)
	err := nc.Read(msg)

	assert.Error(t, err)
	assert.EqualError(t, err, "read error")
}

func TestReadWithEofError(t *testing.T) {
	nc := newNodeConn("baka", "127.0.0.1:12345", _createConn(nil))
	msg := proto.NewMessage()
	req := getReq()
	req.mType = mergeTypeJoin
	req.reply = &resp{}
	req.resp = newresp(respArray, []byte("2"))
	req.resp.array = append(req.resp.array, newresp(respBulk, []byte("3\r\nGET")))
	req.resp.arrayn++
	msg.WithRequest(req)

	err := nc.Read(msg)
	assert.Equal(t, io.EOF, errors.Cause(err))

	rnc := nc.(*nodeConn)
	assert.NoError(t, rnc.Close())
	assert.True(t, rnc.Closed())
	assert.Error(t, rnc.Read(msg))
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
