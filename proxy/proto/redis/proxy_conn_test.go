package redis

import (
	"errors"
	"testing"

	"overlord/proxy/proto"

	"github.com/stretchr/testify/assert"
)

func TestDecodeBasicOk(t *testing.T) {
	data := "*2\r\n$3\r\nGET\r\n$4\r\nbaka\r\n"
	conn := _createConn([]byte(data))
	pc := NewProxyConn(conn)

	msgs := proto.GetMsgs(1)
	nmsgs, err := pc.Decode(msgs)
	assert.NoError(t, err)
	assert.Len(t, nmsgs, 1)

	req := msgs[0].Request().(*Request)
	assert.Equal(t, mergeTypeNo, req.mType)
	assert.Equal(t, 2, req.resp.arrayn)
	assert.Equal(t, "GET", req.CmdString())
	assert.Equal(t, []byte("GET"), req.Cmd())
	assert.Equal(t, "baka", string(req.Key()))
	assert.Equal(t, []byte("2"), req.resp.data)
	assert.Equal(t, []byte("3\r\nGET"), req.resp.array[0].data)
	assert.Equal(t, []byte("4\r\nbaka"), req.resp.array[1].data)
}

func TestDecodeComplexOk(t *testing.T) {
	data := "*3\r\n$4\r\nMGET\r\n$4\r\nbaka\r\n$4\r\nkaba\r\n*5\r\n$4\r\nMSET\r\n$1\r\na\r\n$1\r\nb\r\n$3\r\neee\r\n$5\r\n12345\r\n*3\r\n$4\r\nMGET\r\n$4\r\nenen\r\n$4\r\nnime\r\n*2\r\n$3\r\nGET\r\n$5\r\nabcde\r\n*3\r\n$3\r\nDEL\r\n$1\r\na\r\n$1\r\nb\r\n"
	conn := _createConn([]byte(data))
	pc := NewProxyConn(conn)
	// test reuse command
	msgs := proto.GetMsgs(16)
	msgs[1].WithRequest(getReq())
	msgs[1].WithRequest(getReq())
	msgs[1].Reset()
	msgs[2].WithRequest(getReq())
	msgs[2].WithRequest(getReq())
	msgs[2].WithRequest(getReq())
	msgs[2].Reset()
	// decode
	nmsgs, err := pc.Decode(msgs)
	assert.NoError(t, err)
	assert.Len(t, nmsgs, 5)
	// MGET baka
	assert.Len(t, nmsgs[0].Batch(), 2)
	req := msgs[0].Requests()[0].(*Request)
	assert.Equal(t, mergeTypeJoin, req.mType)
	assert.Equal(t, 2, req.resp.arrayn)
	assert.Equal(t, "GET", req.CmdString())
	assert.Equal(t, []byte("GET"), req.Cmd())
	assert.Equal(t, "baka", string(req.Key()))
	assert.Equal(t, []byte("2"), req.resp.data)
	assert.Equal(t, []byte("3\r\nGET"), req.resp.array[0].data)
	assert.Equal(t, []byte("4\r\nbaka"), req.resp.array[1].data)
	// MGET kaba
	req = msgs[0].Requests()[1].(*Request)
	assert.Equal(t, mergeTypeJoin, req.mType)
	assert.Equal(t, 2, req.resp.arrayn)
	assert.Equal(t, "GET", req.CmdString())
	assert.Equal(t, []byte("GET"), req.Cmd())
	assert.Equal(t, "kaba", string(req.Key()))
	assert.Equal(t, []byte("2"), req.resp.data)
	assert.Equal(t, []byte("3\r\nGET"), req.resp.array[0].data)
	assert.Equal(t, []byte("4\r\nkaba"), req.resp.array[1].data)
	// MSET a b
	assert.Len(t, nmsgs[1].Batch(), 2)
	req = msgs[1].Requests()[0].(*Request)
	assert.Equal(t, mergeTypeOK, req.mType)
	assert.Equal(t, 3, req.resp.arrayn)
	assert.Equal(t, "MSET", req.CmdString())
	assert.Equal(t, []byte("MSET"), req.Cmd())
	assert.Equal(t, "a", string(req.Key()))
	assert.Equal(t, []byte("3"), req.resp.data)
	assert.Equal(t, []byte("4\r\nMSET"), req.resp.array[0].data)
	assert.Equal(t, []byte("1\r\na"), req.resp.array[1].data)
	assert.Equal(t, []byte("1\r\nb"), req.resp.array[2].data)
	// MSET eee 12345
	req = msgs[1].Requests()[1].(*Request)
	assert.Equal(t, mergeTypeOK, req.mType)
	assert.Equal(t, 3, req.resp.arrayn)
	assert.Equal(t, "MSET", req.CmdString())
	assert.Equal(t, []byte("MSET"), req.Cmd())
	assert.Equal(t, "eee", string(req.Key()))
	assert.Equal(t, []byte("3"), req.resp.data)
	assert.Equal(t, []byte("4\r\nMSET"), req.resp.array[0].data)
	assert.Equal(t, []byte("3\r\neee"), req.resp.array[1].data)
	assert.Equal(t, []byte("5\r\n12345"), req.resp.array[2].data)
	// MGET enen
	assert.Len(t, nmsgs[0].Batch(), 2)
	req = msgs[2].Requests()[0].(*Request)
	assert.Equal(t, mergeTypeJoin, req.mType)
	assert.Equal(t, 2, req.resp.arrayn)
	assert.Equal(t, "GET", req.CmdString())
	assert.Equal(t, []byte("GET"), req.Cmd())
	assert.Equal(t, "enen", string(req.Key()))
	assert.Equal(t, []byte("2"), req.resp.data)
	assert.Equal(t, []byte("3\r\nGET"), req.resp.array[0].data)
	assert.Equal(t, []byte("4\r\nenen"), req.resp.array[1].data)
	// MGET nime
	req = msgs[2].Requests()[1].(*Request)
	assert.Equal(t, mergeTypeJoin, req.mType)
	assert.Equal(t, 2, req.resp.arrayn)
	assert.Equal(t, "GET", req.CmdString())
	assert.Equal(t, []byte("GET"), req.Cmd())
	assert.Equal(t, "nime", string(req.Key()))
	assert.Equal(t, []byte("2"), req.resp.data)
	assert.Equal(t, []byte("3\r\nGET"), req.resp.array[0].data)
	assert.Equal(t, []byte("4\r\nnime"), req.resp.array[1].data)
	// GET abcde
	req = msgs[3].Requests()[0].(*Request)
	assert.Equal(t, mergeTypeNo, req.mType)
	assert.Equal(t, 2, req.resp.arrayn)
	assert.Equal(t, "GET", req.CmdString())
	assert.Equal(t, []byte("GET"), req.Cmd())
	assert.Equal(t, "abcde", string(req.Key()))
	assert.Equal(t, []byte("2"), req.resp.data)
	assert.Equal(t, []byte("3\r\nGET"), req.resp.array[0].data)
	assert.Equal(t, []byte("5\r\nabcde"), req.resp.array[1].data)

	req = msgs[4].Requests()[0].(*Request)
	assert.Equal(t, mergeTypeCount, req.mType)
	assert.Equal(t, 2, req.resp.arrayn)
	assert.Equal(t, "DEL", req.CmdString())
	assert.Equal(t, "a", string(req.Key()))
	assert.Equal(t, []byte("2"), req.resp.data)
}

func TestEncodeNotSupportCtl(t *testing.T) {
	msg := proto.NewMessage()
	req := getReq()
	req.resp = &resp{
		rTp:  respArray,
		data: []byte("2"),
		array: []*resp{
			&resp{
				rTp:  respBulk,
				data: []byte("3\r\nfoo"),
			},
			&resp{
				rTp:  respBulk,
				data: []byte("4\r\nbara"),
			},
		},
		arrayn: 2,
	}
	msg.WithRequest(req)
	conn := _createConn([]byte(nil))
	pc := NewProxyConn(conn)
	err := pc.Encode(msg)
	assert.NoError(t, err)
	assert.Equal(t, req.reply.data, notSupportDataBytes)

	req.resp.next()
	req.resp.array[0].data = cmdPingBytes
	err = pc.Encode(msg)
	assert.NoError(t, err)
	assert.Equal(t, req.reply.data, pongDataBytes)

	req.resp.array[0].data = cmdQuitBytes
	err = pc.Encode(msg)
	assert.NoError(t, err)
	assert.Equal(t, req.reply.data, justOkBytes)
}

func TestEncodeMergeOk(t *testing.T) {
	ts := []struct {
		Name   string
		MType  mergeType
		Reply  []*resp
		Expect string
	}{
		{
			Name:  "mergeNotSupport",
			MType: mergeTypeNo,
			Reply: []*resp{
				&resp{
					rTp:  respString,
					data: []byte("123456789"),
				},
			},
			Expect: "-Error: command not support\r\n",
		},
		{
			Name:  "mergeOK",
			MType: mergeTypeOK,
			Reply: []*resp{
				&resp{
					rTp:  respString,
					data: []byte("OK"),
				},
				&resp{
					rTp:  respString,
					data: []byte("OK"),
				},
			},
			Expect: "+OK\r\n",
		},
		{
			Name:  "mergeCount",
			MType: mergeTypeCount,
			Reply: []*resp{
				&resp{
					rTp:  respInt,
					data: []byte("1"),
				},
				&resp{
					rTp:  respInt,
					data: []byte("1"),
				},
			},
			Expect: ":2\r\n",
		},
		{
			Name:  "mergeJoin",
			MType: mergeTypeJoin,
			Reply: []*resp{
				&resp{
					rTp:  respString,
					data: []byte("abc"),
				},
				&resp{
					rTp:  respString,
					data: []byte("ooo"),
				},
				&resp{
					rTp:  respString,
					data: []byte("mmm"),
				},
			},
			Expect: "*3\r\n+abc\r\n+ooo\r\n+mmm\r\n",
		},
	}
	for _, tt := range ts {
		t.Run(tt.Name, func(t *testing.T) {
			msg := proto.NewMessage()
			for _, rpl := range tt.Reply {
				req := getReq()
				req.mType = tt.MType
				req.reply = rpl
				msg.WithRequest(req)
			}
			if msg.IsBatch() {
				msg.Batch()
			}
			conn, buf := _createDownStreamConn()
			pc := NewProxyConn(conn)
			err := pc.Encode(msg)
			if !assert.NoError(t, err) {
				return
			}
			err = pc.Flush()
			if !assert.NoError(t, err) {
				return
			}
			data := make([]byte, 2048)
			size, err := buf.Read(data)
			assert.NoError(t, err)
			assert.Equal(t, tt.Expect, string(data[:size]))

		})
	}
}

func TestEncodeWithError(t *testing.T) {
	msg := proto.NewMessage()
	req := getReq()
	req.mType = mergeTypeNo
	req.reply = nil
	msg.WithRequest(req)
	mockErr := errors.New("baka error")
	msg.WithError(mockErr)
	msg.Done()

	conn, buf := _createDownStreamConn()
	pc := NewProxyConn(conn)
	err := pc.Encode(msg)
	assert.Error(t, err)
	assert.Equal(t, mockErr, err)

	err = pc.Flush()
	assert.NoError(t, err)

	data := make([]byte, 2048)
	size, err := buf.Read(data)
	assert.NoError(t, err)
	assert.Equal(t, "-baka error\r\n", string(data[:size]))
}

func TestEncodeWithPing(t *testing.T) {
	msg := proto.NewMessage()
	req := getReq()
	req.mType = mergeTypeNo
	req.resp = &resp{
		rTp: respArray,
		array: []*resp{
			&resp{
				rTp:  respBulk,
				data: []byte("4\r\nPING"),
			},
		},
		arrayn: 1,
	}
	req.reply = &resp{}
	msg.WithRequest(req)

	conn, buf := _createDownStreamConn()
	pc := NewProxyConn(conn)
	err := pc.Encode(msg)
	assert.NoError(t, err)
	err = pc.Flush()
	assert.NoError(t, err)

	data := make([]byte, 2048)
	size, err := buf.Read(data)
	assert.NoError(t, err)
	assert.Equal(t, "+PONG\r\n", string(data[:size]))
}
