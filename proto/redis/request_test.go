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

func TestRequestEmptyCmdAndKeys(t *testing.T) {
	r := newrespArray([]*resp{})
	req := newReq()
	req.resp = r
	assert.Len(t, req.Cmd(), 0)
	assert.Len(t, req.Key(), 0)
	r = newrespArray([]*resp{newresp(respString, []byte("baka"))})
	req.resp = r
	assert.Equal(t, []byte("baka"), req.Key())
	req.Put()
	getReq()
}

func TestRequestIsCtlAlwaysFalse(t *testing.T) {
	req := newReq()
	req.resp = newrespArray([]*resp{})
	assert.False(t, req.isCtl())
}
