package redis

import (
	"fmt"
	"overlord/proto"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDecodeBasicOk(t *testing.T) {
	msgs := proto.GetMsgSlice(16)
	data := "*2\r\n$3\r\nGET\r\n$4\r\nbaka\r\n"
	conn := _createConn([]byte(data))
	pc := NewProxyConn(conn)

	nmsgs, err := pc.Decode(msgs)
	assert.NoError(t, err)
	assert.Len(t, nmsgs, 1)
}

func TestDecodeComplexOk(t *testing.T) {
	msgs := proto.GetMsgSlice(16)
	data := "*3\r\n$4\r\nMGET\r\n$4\r\nbaka\r\n$4\r\nkaba\r\n*5\r\n$4\r\nMSET\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nb\r\n$1\r\nc\r\n*3\r\n$4\r\nMGET\r\n$4\r\nbaka\r\n$4\r\nkaba\r\n"
	conn := _createConn([]byte(data))
	pc := NewProxyConn(conn)
	// test reuse command
	msgs[1].WithRequest(NewRequest("get", "a"))
	msgs[1].WithRequest(NewRequest("get", "a"))
	msgs[1].Reset()
	msgs[2].WithRequest(NewRequest("get", "a"))
	msgs[2].WithRequest(NewRequest("get", "a"))
	msgs[2].Reset()
	nmsgs, err := pc.Decode(msgs)
	assert.NoError(t, err)
	assert.Len(t, nmsgs, 3)
	assert.Len(t, nmsgs[0].Batch(), 2)
}

func TestEncodeCmdOk(t *testing.T) {

	ts := []struct {
		Name   string
		Reps   []*resp
		Obj    *resp
		Expect string
	}{
		{
			Name:   "MergeJoinOk",
			Reps:   []*resp{newRESPBulk([]byte("3\r\nabc")), newRESPNull(respBulk)},
			Obj:    newRESPArray([]*resp{newRESPBulk([]byte("4\r\nMGET")), newRESPBulk([]byte("3\r\nABC")), newRESPBulk([]byte("3\r\nxnc"))}),
			Expect: "*2\r\n$3\r\nabc\r\n$-1\r\n",
		},
		{
			Name: "MergeCountOk",
			Reps: []*resp{newRESPInt(1), newRESPInt(1), newRESPInt(0)},
			Obj: newRESPArray(
				[]*resp{
					newRESPBulk([]byte("3\r\nDEL")),
					newRESPBulk([]byte("1\r\na")),
					newRESPBulk([]byte("2\r\nab")),
					newRESPBulk([]byte("3\r\nabc")),
				}),
			Expect: ":2\r\n",
		},
		{
			Name: "MergeCountOk",
			Reps: []*resp{newRESPString([]byte("OK")), newRESPString([]byte("OK"))},
			Obj: newRESPArray(
				[]*resp{
					newRESPBulk([]byte("4\r\nMSET")),
					newRESPBulk([]byte("1\r\na")),
					newRESPBulk([]byte("2\r\nab")),
					newRESPBulk([]byte("3\r\nabc")),
					newRESPBulk([]byte("4\r\nabcd")),
				}),
			Expect: "+OK\r\n",
		},
	}
	for _, tt := range ts {
		t.Run(tt.Name, func(t *testing.T) {
			rs := tt.Reps
			msg := proto.NewMessage()
			co := tt.Obj
			if isComplex(co.nth(0).data) {
				err := newSubCmd(msg, co)
				if assert.NoError(t, err) {
					for i, req := range msg.Requests() {
						cmd := req.(*Request)
						cmd.reply = rs[i]
					}
					msg.Batch()
				}
			} else {
				cmd := newRequest(co)
				cmd.reply = rs[0]
				msg.WithRequest(cmd)
			}
			data := make([]byte, 2048)
			conn, buf := _createDownStreamConn()
			pc := NewProxyConn(conn)
			err := pc.Encode(msg)
			if assert.NoError(t, err) {
				size, _ := buf.Read(data)
				assert.NoError(t, err)
				assert.Equal(t, tt.Expect, string(data[:size]))
			}
		})
	}

}
func TestEncodeErr(t *testing.T) {
	data := make([]byte, 2048)
	conn, buf := _createDownStreamConn()
	pc := NewProxyConn(conn)
	msg := proto.NewMessage()
	msg.DoneWithError(fmt.Errorf("ERR msg err"))
	err := pc.Encode(msg)
	assert.NoError(t, err)
	size, _ := buf.Read(data)
	assert.Equal(t, "-ERR msg err\r\n", string(data[:size]))
}
