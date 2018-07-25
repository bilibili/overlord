package redis

import (
	"io"
	"testing"

	"overlord/lib/bufio"
	"overlord/proto"

	"github.com/stretchr/testify/assert"
)

func _createRespConn(data []byte) *respConn {
	conn := _createConn(data)
	return newRESPConn(conn)
}

func _runDecodeResps(t *testing.T, line string, expect string, rtype respType, eErr error) {
	rc := _createRespConn([]byte(line))
	err := rc.br.Read()
	if assert.NoError(t, err) {
		robj := &resp{}
		err = rc.decodeRESP(robj)
		if eErr == nil {
			if assert.NoError(t, err) {
				if expect == "" {
					assert.Nil(t, robj.data)
				} else {
					assert.Equal(t, expect, string(robj.data))
				}
				assert.Equal(t, rtype, robj.rtype)
			}
		} else {
			if assert.Error(t, err) {
				assert.Equal(t, eErr, err)
			}
		}

	}
}

func TestDecodeRESP(t *testing.T) {
	tslist := []struct {
		Name   string
		Input  string
		Expect string
		Rtype  respType
		EErr   error
	}{
		{"RespStringOk", "+Bilibili 干杯 - ( ゜- ゜)つロ\r\n", "Bilibili 干杯 - ( ゜- ゜)つロ", respString, nil},
		{"RespStringWithLF", "+Bilibili\n 干杯 - ( ゜- ゜)つロ\r\n", "Bilibili\n 干杯 - ( ゜- ゜)つロ", respString, nil},
		{"RespErrorOk", "-Bilibili 干杯 - ( ゜- ゜)つロ\r\n", "Bilibili 干杯 - ( ゜- ゜)つロ", respError, nil},
		{"RespIntOk", ":10\r\n", "10", respInt, nil},
		// {"RespIntWrongNumber", ":a@#\r\n", "", respInt, nil}, // now it's can't be checked
		{"RespBulkOk", "$35\r\nBilibili 干杯 - ( ゜- ゜)つロ\r\n", "35\r\nBilibili 干杯 - ( ゜- ゜)つロ", respBulk, nil},
		{"RespBulkNullOk", "$-1\r\n", "", respBulk, nil},
		{"RespBulkWrongSizeError", "$37\r\nBilibili 干杯 - ( ゜- ゜)つロ\r\n", "", respBulk, bufio.ErrBufferFull},

		{"RespArrayOk", "*3\r\n$2\r\nab\r\n+baka lv9\r\n-ServerError:deepn dark fantasy\r\n", "3", respArray, nil},
		{"RespArrayNotFull", "*3\r\n$30000\r\nab\r\n+baka lv9\r\n-ServerError:deepn dark fantasy\r\n", "", respArray, bufio.ErrBufferFull},
		{"RespArrayNullOk", "*-1\r\n", "", respArray, nil},
	}
	for _, tt := range tslist {
		t.Run(tt.Name, func(t *testing.T) {
			_runDecodeResps(t, tt.Input, tt.Expect, tt.Rtype, tt.EErr)
		})
	}
}

func TestDecodeMsgReachMaxOk(t *testing.T) {
	line := []byte("*2\r\n$3\r\nget\r\n$1\r\na\r\n*2\r\n$3\r\nget\r\n$1\r\na\r\n")
	rc := _createRespConn([]byte(line))
	msgs := []*proto.Message{proto.NewMessage(), proto.NewMessage()}
	rs, err := rc.decodeMsg(msgs)
	assert.NoError(t, err)
	assert.Len(t, rs, 2)

	assert.Equal(t, respArray, rs[0].Request().(*Request).respObj.rtype)
	assert.Equal(t, 2, rs[1].Request().(*Request).respObj.Len())
}

func TestDecodeMsgReachFullOk(t *testing.T) {
	line := []byte("*2\r\n$3\r\nget\r\n$1\r\na\r\n*2\r\n$3\r\nget\r\n$1")
	rc := _createRespConn([]byte(line))
	msgs := []*proto.Message{proto.NewMessage(), proto.NewMessage()}
	rs, err := rc.decodeMsg(msgs)
	assert.NoError(t, err)
	assert.Len(t, rs, 1)
	assert.Equal(t, respArray, rs[0].Request().(*Request).respObj.rtype)
}

func TestDecodeMsgReuseRESP(t *testing.T) {
	line := []byte("*2\r\n$3\r\nget\r\n$1\r\na\r\n*2\r\n$3\r\nget\r\n$1\r\na\r\n")
	rc := _createRespConn([]byte(line))
	msg0 := proto.NewMessage()
	msg0.WithRequest(NewRequest("get", "a"))
	msg0.Reset()
	msg1 := proto.NewMessage()
	msg1.WithRequest(NewRequest("get", "a"))
	msg1.Reset()
	msgs := []*proto.Message{msg0, msg1}
	rs, err := rc.decodeMsg(msgs)
	assert.NoError(t, err)
	assert.Len(t, rs, 2)

	assert.Equal(t, respArray, rs[0].Request().(*Request).respObj.rtype)
	assert.Equal(t, 2, rs[1].Request().(*Request).respObj.Len())
}
func TestDecodeCountOk(t *testing.T) {
	line := []byte("$1\r\na\r\n+my name is\r\n")
	rc := _createRespConn([]byte(line))
	rs := []*resp{&resp{}, &resp{}}
	err := rc.decodeCount(rs)
	assert.NoError(t, err)
	assert.Len(t, rs, 2)
	assert.Equal(t, respBulk, rs[0].rtype)
	assert.Equal(t, respString, rs[1].rtype)
}

func TestDecodeCountNotFull(t *testing.T) {
	line := []byte("$1\r\na\r\n+my name is\r\n")
	rc := _createRespConn([]byte(line))
	rs := []*resp{&resp{}, &resp{}, &resp{}}
	err := rc.decodeCount(rs)
	assert.Error(t, err)
	assert.Equal(t, io.EOF, err)
}

func TestEncodeResp(t *testing.T) {
	ts := []struct {
		Name   string
		Robj   *resp
		Expect string
	}{
		{Name: "IntOk", Robj: newRESPInt(1024), Expect: ":1024\r\n"},
		{Name: "StringOk", Robj: newRESPString([]byte("baka")), Expect: "+baka\r\n"},
		{Name: "ErrorOk", Robj: newRESPPlain(respError, []byte("kaba")), Expect: "-kaba\r\n"},

		{Name: "BulkOk", Robj: newRESPBulk([]byte("4\r\nkaba")), Expect: "$4\r\nkaba\r\n"},
		{Name: "BulkNullOk", Robj: newRESPNull(respBulk), Expect: "$-1\r\n"},

		{Name: "ArrayNullOk", Robj: newRESPNull(respArray), Expect: "*-1\r\n"},
		{Name: "ArrayOk",
			Robj:   newRESPArray([]*resp{newRESPBulk([]byte("2\r\nka")), newRESPString([]byte("baka"))}),
			Expect: "*2\r\n$2\r\nka\r\n+baka\r\n"},
	}

	for _, tt := range ts {
		t.Run(tt.Name, func(t *testing.T) {
			sock, buf := _createDownStreamConn()
			conn := newRESPConn(sock)
			// err := conn.encode(newRespInt(1024))
			err := tt.Robj.encode(conn.bw)
			assert.NoError(t, err)
			err = conn.Flush()
			assert.NoError(t, err)
			data := make([]byte, 1024)
			n, err := buf.Read(data)
			assert.NoError(t, err)
			assert.Equal(t, tt.Expect, string(data[:n]))
		})

	}
}
