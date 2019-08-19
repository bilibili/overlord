package redis

import (
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

func BenchmarkCmdTypeCheck(b *testing.B) {
	req := getReq()
	req.resp.array = append(req.resp.array, &resp{
		data: []byte("3\r\nSET"),
	})
	for i := 0; i < b.N; i++ {
		req.IsSupport()
	}
}
