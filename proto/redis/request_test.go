package redis

import (
	"testing"

	"overlord/lib/bufio"

	"github.com/stretchr/testify/assert"
)

func TestRequestNewRequest(t *testing.T) {
	var bs = []byte("*2\r\n$4\r\nLLEN\r\n$6\r\nmylist\r\n")
	// conn
	conn := _createConn(bs)
	br := bufio.NewReader(conn, bufio.Get(1024))
	br.Read()
	req := getReq()
	err := req.resp.decode(br)
	assert.Nil(t, err)
	assert.Equal(t, mergeTypeNo, req.mType)
	assert.Equal(t, 2, req.resp.arrayn)
	assert.Equal(t, "LLEN", req.CmdString())
	assert.Equal(t, []byte("LLEN"), req.Cmd())
	assert.Equal(t, "mylist", string(req.Key()))
}


var cmds = []string{
	"4\r\nDUMP", "6\r\nEXISTS", "4\r\nPTTL",
	"6\r\nSELECT", "4\r\nTIME", "6\r\nCONFIG",
	"4\r\nHSET", "6\r\nHSETNX", "7\r\nLINSERT",
	"4\r\nQUIT",
}

func str2Request(key string) *Request {
	r := &Request{
		resp: &resp{
			rTp:    respArray,
			data:   []byte("1"),
			arrayn: 1,
			array: []*resp{
				&resp{
					rTp:    respBulk,
					data:   []byte(key),
					array:  nil,
					arrayn: 0,
				},
			},
		},
		reply: nil,
	}
	return r
}

func BenchmarkRequestIsSupportByMap(b *testing.B) {
	b.StopTimer()
	reqs := make([]*Request, len(cmds))
	for i, key := range cmds {
		reqs[i] = str2Request(key)
	}
	b.StartTimer()

	for i := 0; i < 1000000; i++ {
		for _, req := range reqs {
			req.IsSupport()
		}
	}
}
