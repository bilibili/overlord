package memcache

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	"overlord/pkg/mockconn"
	libcon "overlord/pkg/net"
	"overlord/pkg/types"
	"overlord/proxy/proto"

	"github.com/stretchr/testify/assert"
)

func TestParseLenGetsResponseOk(t *testing.T) {
	x := []byte(" 0 11 22\r\n")
	i, err := parseLen(x, 2)
	assert.NoError(t, err)
	assert.Equal(t, 11, i)

	x = []byte(" 1024 11\r\n")
	i, err = parseLen(x, 2)
	assert.NoError(t, err)
	assert.Equal(t, 11, i)

	i, err = parseLen([]byte("VALUE a 0 1 \r\n"), 4)
	assert.NoError(t, err)
	assert.Equal(t, 1, i)

	i, err = parseLen([]byte("VALUE 0 1024 reply\r\n"), 3)
	assert.NoError(t, err)
	assert.Equal(t, 1024, i)

	i, err = parseLen([]byte("VALUE 0 1 1024 1023 reply\r\n"), 4)
	assert.NoError(t, err)
	assert.Equal(t, 1024, i)
}

func TestParseLenParseLengthError(t *testing.T) {
	i, err := parseLen([]byte("VALUE 0 abcdefghich@asaeaw\r\n"), 3)
	assert.Error(t, err)
	assert.Equal(t, ErrBadLength, err)
	assert.Equal(t, -1, i)

	i, err = parseLen([]byte("VALUE\r\n"), 2)
	assert.Error(t, err)
	assert.Equal(t, ErrBadLength, err)
	assert.Equal(t, -1, i)

	i, err = parseLen([]byte("VALUE 0\r\n"), 0)
	assert.Error(t, err)
	assert.Equal(t, ErrBadLength, err)
	assert.Equal(t, -1, i)

}

func TestNextFeild(t *testing.T) {
	b, e := nextField([]byte(" "))
	assert.Equal(t, 1, b)
	assert.Equal(t, 1, e)

	b, e = nextField([]byte("get\r\n"))
	assert.Equal(t, 0, b)
	assert.Equal(t, 3, e)

	b, e = nextField([]byte("get a\r\n"))
	assert.Equal(t, 0, b)
	assert.Equal(t, 3, e)
}

func TestLegalKeyOk(t *testing.T) {
	assert.False(t, legalKey(bytes.Repeat([]byte("abcde"), 51)))
	assert.False(t, legalKey([]byte{0x7f}))
	assert.False(t, legalKey([]byte{byte(' ')}))
	assert.True(t, legalKey([]byte("baka")))
}

func TestProxyConnDecodeOk(t *testing.T) {
	ts := []struct {
		Name string
		Data string
		Err  error
		Key  string
		Cmd  string
	}{
		// set cases
		{"SetOk", "set mykey 0 0 2\r\nab\r\n", nil, "mykey", "set"},
		{"SetBadKey", "set my" + string([]byte{0x7f}) + "key 0 0 2\r\nab\r\n", ErrBadKey, "", ""},
		{"SetBadLength", "set mykey 0 0 abcdef\r\nab\r\n", ErrBadLength, "", ""},
		{"SetBodyTooShort", "set mykey 0 0 2\r\na\r\n", nil, "mykey", "set"},
		{"SetWithNoCRLF", "set mykey 0 0 2\r\nabba", ErrBadRequest, "", ""},

		// replace
		{"ReplaceOk", "replace mykey 0 0 2\r\nab\r\n", nil, "mykey", "replace"},

		// add
		{"AddOk", "add mykey 0 0 2\r\nab\r\n", nil, "mykey", "add"},

		// append
		{"AppendOk", "append mykey 0 0 2\r\nab\r\n", nil, "mykey", "append"},

		// prepend
		{"PrependOk", "prepend mykey 0 0 2\r\nab\r\n", nil, "mykey", "prepend"},

		// cas
		{"CasOk", "cas mykey 0 0 2 47\r\nab\r\n", nil, "mykey", "cas"},

		// Get Gets
		{"GetOk", "get mykey\r\n", nil, "mykey", "get"},
		{"GetMultiKeyOk", "get mykey yourkey\r\n", nil, "mykey", "get"},
		{"GetBadKey", "get my" + string([]byte{0x7f}) + "key\r\n", ErrBadKey, "", ""},
		{"GetsOk", "gets mykey\r\n", nil, "mykey", "gets"},
		{"GetsMultiKeyOk", "gets mykey yourkey yuki\r\n", nil, "mykey", "gets"},

		// Delete
		{"DeleteOk", "delete mykey\r\n", nil, "mykey", "delete"},
		{"DeleteIllegalKey", "delete my" + string([]byte{0x7f}) + "key\r\n", ErrBadKey, "", ""},

		// Incr/Decr
		{"IncrOk", "incr mykey 10\r\n", nil, "mykey", "incr"},
		{"IncrIllegalKey", "incr my" + string([]byte{0x7f}) + "key 10\r\n", ErrBadKey, "", ""},
		{"IncrBadNumber", "incr mykey abcde\r\n", ErrBadRequest, "", ""},
		{"DecrOk", "decr mykey 10\r\n", nil, "mykey", "decr"},

		// Touch
		{"TouchOk", "touch mykey 10\r\n", nil, "mykey", "touch"},
		{"TouchZeroExpireOk", "touch mykey 0\r\n", nil, "mykey", "touch"},
		{"TouchBadLength", "touch mykey abdef\r\n", ErrBadRequest, "", ""},
		{"TouchBadKey", "touch my" + string([]byte{0x7f}) + "key 10\r\n", ErrBadKey, "", ""},

		// Gat Gats
		{"GatOk", "gat 10 mykey\r\n", nil, "mykey", "gat"},
		{"GatMultiKeyOk", "gat 10 mykey yourkey\r\n", nil, "mykey", "gat"},
		{"GatBadKey", "gat 10 my" + string([]byte{0x7f}) + "key\r\n", ErrBadKey, "", ""},
		{"GatBadExpire", "gat abcdef mykey\r\n", ErrBadRequest, "", ""},
		{"GatsOk", "gats 10 mykey\r\n", nil, "mykey", "gats"},
		{"GatsMultiKeyOk", "gats 10 mykey yourkey yuki\r\n", nil, "mykey", "gats"},
		// Not support
		{"NotSupportCmd", "baka 10 mykey\r\n", ErrBadRequest, "", ""},
		// {"NotFullLine", "baka 10", ErrBadRequest, "", ""},
	}

	for _, tt := range ts {
		t.Run(tt.Name, func(t *testing.T) {
			conn := libcon.NewConn(mockconn.CreateConn([]byte(tt.Data), 1), time.Second, time.Second)
			p := NewProxyConn(conn)
			mlist := proto.GetMsgs(2)
			// test req reuse.
			mlist[0].WithRequest(NewReq())
			mlist[0].Reset()
			msgs, err := p.Decode(mlist)

			if tt.Err != nil {
				_causeEqual(t, tt.Err, err)
			} else {
				assert.NoError(t, err)
				if err != nil {
					m := msgs[0]
					assert.NotNil(t, m)
					assert.NotNil(t, m.Request())
					assert.Equal(t, tt.Key, string(m.Request().Key()))
					assert.Equal(t, tt.Cmd, m.Request().Cmd())
				}
			}
		})
	}
}

func _createRespMsg(t *testing.T, req []byte, resps [][]byte) *proto.Message {
	conn := libcon.NewConn(mockconn.CreateConn([]byte(req), 1), time.Second, time.Second)
	p := NewProxyConn(conn)
	mlist := proto.GetMsgs(2)

	_, err := p.Decode(mlist)
	assert.NoError(t, err)
	m := mlist[0]

	if !m.IsBatch() {
		nc := _createNodeConn(resps[0])
		err := nc.Read(m)
		assert.NoError(t, err)
	} else {
		subs := m.Batch()
		for idx, resp := range resps {
			nc := _createNodeConn(resp)
			err := nc.Read(subs[idx])
			assert.NoError(t, err)
		}
	}

	return m
}

func TestProxyConnEncodeOk(t *testing.T) {
	ts := []struct {
		Name   string
		Req    string
		Resp   [][]byte
		Except string
	}{
		{Name: "SetOk", Req: "set mykey 0 0 1\r\na\r\n", Resp: [][]byte{[]byte("STORED\r\n")}, Except: "STORED\r\n"},
		{Name: "GetOk", Req: "get mykey\r\n", Resp: [][]byte{[]byte("VALUE mykey 0 2\r\nab\r\nEND\r\n")}, Except: "VALUE mykey 0 2\r\nab\r\nEND\r\n"},
		{Name: "GetMultiOk", Req: "get mykey yourkey\r\n",
			Resp:   [][]byte{[]byte("VALUE mykey 0 2\r\nab\r\nEND\r\n"), []byte("VALUE yourkey 0 3\r\ncde\r\nEND\r\n")},
			Except: "VALUE mykey 0 2\r\nab\r\nVALUE yourkey 0 3\r\ncde\r\nEND\r\n"},

		{Name: "GetMultiMissOne", Req: "get mykey a key baka\r\n",
			Resp:   [][]byte{[]byte("VALUE mykey 0 2\r\nab\r\nEND\r\n"), endBytes, endBytes, endBytes},
			Except: "VALUE mykey 0 2\r\nab\r\nEND\r\n"},

		{Name: "GetMultiAllMiss", Req: "get nokey1 nokey\r\n",
			Resp:   [][]byte{[]byte("END\r\n"), []byte("END\r\n")},
			Except: "END\r\n"},
	}

	for _, tt := range ts {
		t.Run(tt.Name, func(t *testing.T) {
			conn := libcon.NewConn(mockconn.CreateConn(nil, 1), time.Second, time.Second)
			p := NewProxyConn(conn)
			msg := _createRespMsg(t, []byte(tt.Req), tt.Resp)
			err := p.Encode(msg)
			assert.NoError(t, err)
			err = p.Flush()
			assert.NoError(t, err)
			c := conn.Conn.(*mockconn.MockConn)
			buf := make([]byte, 1024)
			size, err := c.Wbuf.Read(buf)
			assert.NoError(t, err)
			assert.Equal(t, tt.Except, string(buf[:size]))
		})
	}
}

func TestEncodeErr(t *testing.T) {
	conn := libcon.NewConn(mockconn.CreateConn(nil, 1), time.Second, time.Second)
	p := NewProxyConn(conn)

	msg := proto.NewMessage()
	msg.WithError(fmt.Errorf("SERVER_ERR"))
	err := p.Encode(msg)
	assert.NoError(t, err)

	msg = proto.NewMessage()
	msg.Type = types.CacheTypeMemcache
	msg.WithRequest(&mockReq{})
	err = p.Encode(msg)
	assert.NoError(t, err)

	msg = proto.NewMessage()
	msg.Type = types.CacheTypeMemcache
	msg.WithRequest(&mockReq{})
	msg.WithRequest(&mockReq{}) // NOTE: batch
	msg.Batch()
	err = p.Encode(msg)
	assert.NoError(t, err)

	p.Flush()
	c := conn.Conn.(*mockconn.MockConn)
	buf := make([]byte, 1024)
	size, err := c.Wbuf.Read(buf)
	assert.NoError(t, err)
	assert.Contains(t, string(buf[:size]), "SERVER_ERR")
}
