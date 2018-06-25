package memcache

import (
	"bytes"
	"testing"

	"github.com/felixhao/overlord/proto"
	"github.com/stretchr/testify/assert"
)

func TestFindLengthGetsResponseOk(t *testing.T) {
	// x := []byte{86, 65, 76, 85, 69, 32, 97, 95, 49, 49, 32, 48, 32, 49, 32, 51, 56, 51, 55, 13, 10}
	x := []byte(" 0 11 22\r\n")
	i, err := findLength(x, true)
	assert.NoError(t, err)
	assert.Equal(t, 11, i)

	x = []byte(" 0 11\r\n")
	i, err = findLength(x, false)
	assert.NoError(t, err)
	assert.Equal(t, 11, i)
}

func TestFindLengthParseLengthError(t *testing.T) {
	i, err := findLength([]byte("VALUE 0 abcdefghich@asaeaw\r\n"), false)
	assert.Error(t, err)
	assert.Equal(t, ErrBadLength, err)
	assert.Equal(t, -1, i)
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
		{"NotFullLine", "baka 10", ErrBadRequest, "", ""},
		{"NotFullLine", "baka 10", ErrBadRequest, "", ""},
	}

	for _, tt := range ts {
		t.Run(tt.Name, func(t *testing.T) {
			conn := _createConn([]byte(tt.Data))
			p := NewProxyConn(conn)
			m := proto.GetMsg()
			err := p.Read()
			assert.NoError(t, err)

			completed, err := p.Decode(m)
			if !completed {
				return
			}

			if tt.Err != nil {
				_causeEqual(t, tt.Err, err)
			} else {
				assert.NotNil(t, m)
				assert.NotNil(t, m.Request())
				assert.Equal(t, tt.Key, string(m.Request().Key()))
				assert.Equal(t, tt.Cmd, m.Request().Cmd())
			}
		})
	}
}

func _createRespMsg(t *testing.T, req []byte, resps [][]byte) *proto.Message {
	conn := _createConn([]byte(req))
	p := NewProxyConn(conn)
	m := proto.GetMsg()

	err := p.Read()
	assert.NoError(t, err)

	_, err = p.Decode(m)
	assert.NoError(t, err)
	if !m.IsBatch() {
		nc := _createNodeConn(resps[0])
		err := nc.Read(m)
		assert.NoError(t, err)
	} else {
		subs := m.Batch()
		for idx, resp := range resps {
			nc := _createNodeConn(resp)
			err := nc.Read(&subs[idx])
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
		{Name: "GetOk", Req: "get mykey\r\n", Resp: [][]byte{[]byte("VALUE 0 2\r\nab\r\nEND\r\n")}, Except: "VALUE 0 2\r\nab\r\nEND\r\n"},
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
			conn := _createConn(nil)
			p := NewProxyConn(conn)
			msg := _createRespMsg(t, []byte(tt.Req), tt.Resp)
			err := p.Encode(msg)
			assert.NoError(t, err)
			c := conn.Conn.(*mockConn)
			buf := make([]byte, 1024)
			size, err := c.wbuf.Read(buf)
			assert.NoError(t, err)
			assert.Equal(t, tt.Except, string(buf[:size]))
		})
	}
}
