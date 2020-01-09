package redis

import (
	"overlord/proxy/proto"
	"testing"
	"time"

	"overlord/pkg/bufio"
	"overlord/pkg/mockconn"
	libnet "overlord/pkg/net"

	"github.com/stretchr/testify/assert"
)

func TestRequestNewRequest(t *testing.T) {
	var bs = []byte("*2\r\n$4\r\nLLEN\r\n$6\r\nmylist\r\n")
	// conn
	conn := libnet.NewConn(mockconn.CreateConn(bs, 1), time.Second, time.Second)
	br := bufio.NewReader(conn, bufio.Get(1024))
	br.Read()
	req := getReq()
	err := req.resp.decode(br)
	assert.Nil(t, err)
	assert.Equal(t, mergeTypeNo, req.mType)
	assert.Equal(t, 2, req.resp.arraySize)
	assert.Equal(t, "LLEN", req.CmdString())
	assert.Equal(t, []byte("LLEN"), req.Cmd())
	assert.Equal(t, "mylist", string(req.Key()))
	assert.True(t, req.IsSupport())
	assert.False(t, req.IsCtl())
}

func TestMergeRequest(t *testing.T) {
	bs := []byte("*7\r\n$4\r\nMSET\r\n$2\r\nk1\r\n$2\r\nv1\r\n$2\r\nk2\r\n$2\r\nv2\r\n$2\r\nk3\r\n$2\r\nv3\r\n")
	conn := libnet.NewConn(mockconn.CreateConn(bs, 1), time.Second, time.Second)
	pc := NewProxyConn(conn, true)
	// message
	msg := proto.NewMessage()
	msg.WithRequest(getReq())
	msg.WithRequest(getReq())
	msg.WithRequest(getReq())
	msg.Reset()
	// decode
	msgs, err := pc.Decode([]*proto.Message{msg})
	assert.NoError(t, err)
	assert.Len(t, msgs, 1)
	// merge
	msg = msgs[0]
	reqs := msg.Requests()
	mainReq := reqs[0].(*Request)
	err = mainReq.Merge(reqs[1:])
	assert.NoError(t, err)
	assert.Len(t, mainReq.resp.array, 7)
	assert.Equal(t, []byte("7"), mainReq.resp.data)
	assert.Equal(t, []byte("4\r\nMSET"), mainReq.resp.array[0].data)
	assert.Equal(t, []byte("2\r\nk1"), mainReq.resp.array[1].data)
	assert.Equal(t, []byte("2\r\nv1"), mainReq.resp.array[2].data)
	assert.Equal(t, []byte("2\r\nk2"), mainReq.resp.array[3].data)
	assert.Equal(t, []byte("2\r\nv2"), mainReq.resp.array[4].data)
	assert.Equal(t, []byte("2\r\nk3"), mainReq.resp.array[5].data)
	assert.Equal(t, []byte("2\r\nv3"), mainReq.resp.array[6].data)
}

func BenchmarkCmdTypeCheck(b *testing.B) {
	req := getReq()
	req.resp.array = append(req.resp.array, &resp{
		data: []byte("3\r\nSET"),
	})
	for i := 0; i < b.N; i++ {
		req.IsSupport()
	}
}
