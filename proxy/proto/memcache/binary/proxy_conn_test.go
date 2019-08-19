package binary

import (
	"errors"
	"testing"
	"time"

	"overlord/pkg/mockconn"
	libcon "overlord/pkg/net"
	"overlord/proxy/proto"

	"github.com/stretchr/testify/assert"
)

var (
	getTestData = []byte{
		0x80,       // magic
		0x0c,       // cmd
		0x00, 0x03, // key len
		0x00,       // extra len
		0x00,       // data type
		0x00, 0x00, // vbucket
		0x00, 0x00, 0x00, 0x03, // body len
		0x00, 0x00, 0x00, 0x00, // opaque
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // cas
		0x41, 0x42, 0x43, // key: ABC
	}
	getRespTestData = []byte{
		0x81,       // magic
		0x0c,       // cmd
		0x00, 0x03, // key len
		0x04,       // extra len
		0x00,       // data type
		0x00, 0x00, // status
		0x00, 0x00, 0x00, 0x0c, // body len
		0x00, 0x00, 0x00, 0x00, // opaque
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // cas
		0x00, 0x00, 0x00, 0x00, // extra: flag
		0x41, 0x42, 0x43, // key: ABC
		0x41, 0x42, 0x43, 0x44, 0x45, // value: ABCDE
	}
	getMissRespTestData = []byte{
		0x81,       // magic
		0x0c,       // cmd
		0x00, 0x00, // key len
		0x00,       // extra len
		0x00,       // data type
		0x00, 0x01, // status
		0x00, 0x00, 0x00, 0x00, // body len
		0x00, 0x00, 0x00, 0x00, // opaque
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // cas
	}
	setTestData = []byte{
		0x80,       // magic
		0x01,       // cmd
		0x00, 0x03, // key len
		0x08,       // extra len
		0x00,       // data type
		0x00, 0x00, // vbucket
		0x00, 0x00, 0x00, 0x0f, // body len
		0x00, 0x00, 0x00, 0x00, // opaque
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // cas
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // extra: flags, expiration
		0x41, 0x42, 0x43, // key: ABC
		0x41, 0x42, 0x43, 0x44, 0x45, // value: ABCDE
	}
	setRespTestData = []byte{
		0x81,       // magic
		0x01,       // cmd
		0x00, 0x00, // key len
		0x00,       // extra len
		0x00,       // data type
		0x00, 0x00, // status
		0x00, 0x00, 0x00, 0x00, // body len
		0x00, 0x00, 0x00, 0x00, // opaque
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // cas
	}
	delTestData = []byte{
		0x80,       // magic
		0x04,       // cmd
		0x00, 0x03, // key len
		0x00,       // extra len
		0x00,       // data type
		0x00, 0x00, // vbucket
		0x00, 0x00, 0x00, 0x03, // body len
		0x00, 0x00, 0x00, 0x00, // opaque
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // cas
		0x41, 0x42, 0x43, // key: ABC
	}
	incrTestData = []byte{
		0x80,       // magic
		0x05,       // cmd
		0x00, 0x03, // key len
		0x14,       // extra len
		0x00,       // data type
		0x00, 0x00, // vbucket
		0x00, 0x00, 0x00, 0x17, // body len
		0x00, 0x00, 0x00, 0x00, // opaque
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // cas
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // extra: Amount to add / subtract
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // extra: Initial value
		0x00, 0x00, 0x00, 0x00, // extra: Expiration
		0x41, 0x42, 0x43, // key: ABC
	}
	touchTestData = []byte{
		0x80,       // magic
		0x1c,       // cmd
		0x00, 0x03, // key len
		0x00, 0x04, // extra len
		0x00,       // data type
		0x00, 0x00, // vbucket
		0x00, 0x00, 0x00, 0x7, // body len
		0x00, 0x00, 0x00, 0x00, // opaque
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // cas
		0x00, 0x00, 0x00, 0x00, // extra: Expiration
		0x41, 0x42, 0x43, // key: ABC
	}
	getQTestData = []byte{
		0x80,       // magic
		0x0d,       // cmd
		0x00, 0x03, // key len
		0x00,       // extra len
		0x00,       // data type
		0x00, 0x00, // vbucket
		0x00, 0x00, 0x00, 0x03, // body len
		0x00, 0x00, 0x00, 0x00, // opaque
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // cas
		0x41, 0x42, 0x43, // key: ABC

		0x80,       // magic
		0x0d,       // cmd
		0x00, 0x03, // key len
		0x00,       // extra len
		0x00,       // data type
		0x00, 0x00, // vbucket
		0x00, 0x00, 0x00, 0x03, // body len
		0x00, 0x00, 0x00, 0x00, // opaque
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // cas
		0x58, 0x59, 0x5A, // key: XYZ

		0x80,       // magic
		0x0c,       // cmd
		0x00, 0x03, // key len
		0x00, 0x00, // extra len
		0x00,       // data type
		0x00, 0x00, // vbucket
		0x00, 0x00, 0x00, 0x03, // body len
		0x00, 0x00, 0x00, 0x00, // opaque
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // cas
		0x61, 0x62, 0x63, // key: abc
	}
	getQRespTestData = [][]byte{
		[]byte{
			0x81,       // magic
			0x0d,       // cmd
			0x00, 0x03, // key len
			0x04,       // extra len
			0x00,       // data type
			0x00, 0x00, // status
			0x00, 0x00, 0x00, 0x0c, // body len
			0x00, 0x00, 0x00, 0x00, // opaque
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // cas
			0x00, 0x00, 0x00, 0x00, // extra: flag
			0x41, 0x42, 0x43, // key: ABC
			0x41, 0x42, 0x43, 0x44, 0x45, // value: ABCDE
		},
		[]byte{
			0x81,       // magic
			0x0d,       // cmd
			0x00, 0x03, // key len
			0x04,       // extra len
			0x00,       // data type
			0x00, 0x00, // status
			0x00, 0x00, 0x00, 0x0c, // body len
			0x00, 0x00, 0x00, 0x00, // opaque
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // cas
			0x00, 0x00, 0x00, 0x00, // extra: flag
			0x58, 0x59, 0x5A, // key: XYZ
			0x56, 0x57, 0x58, 0x59, 0x5a, // value: VWXYZ
		},
		[]byte{
			0x81,       // magic
			0x0c,       // cmd
			0x00, 0x03, // key len
			0x04,       // extra len
			0x00,       // data type
			0x00, 0x00, // status
			0x00, 0x00, 0x00, 0x0c, // body len
			0x00, 0x00, 0x00, 0x00, // opaque
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // cas
			0x00, 0x00, 0x00, 0x00, // extra: flag
			0x61, 0x62, 0x63, // key: abc
			0x61, 0x62, 0x63, 0x64, 0x65, // value: abcde
		},
	}
	getQMissRespTestData = []byte{
		0x81,       // magic
		0x0d,       // cmd
		0x00, 0x00, // key len
		0x00,       // extra len
		0x00,       // data type
		0x00, 0x01, // status
		0x00, 0x00, 0x00, 0x00, // body len
		0x00, 0x00, 0x00, 0x00, // opaque
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // cas
	}
	notTestData = []byte{
		0x80,       // magic
		0xff,       // cmd
		0x00, 0x00, // key len
		0x00,       // extra len
		0x00,       // data type
		0x00, 0x00, // vbucket
		0x00, 0x00, 0x00, 0x0, // body len
		0x00, 0x00, 0x00, 0x00, // opaque
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // cas
	}
)

func TestParseHeader(t *testing.T) {
	req := newReq()
	parseHeader(getTestData, req, true)
	assert.Equal(t, byte(0x80), req.magic)
	assert.Equal(t, []byte{0xc}, req.respType.Bytes())
	assert.Equal(t, []byte{0x00, 0x03}, req.keyLen)
	assert.Equal(t, []byte{0x00}, req.extraLen)
	assert.Equal(t, []byte{0x00, 0x00, 0x00, 0x03}, req.bodyLen)
	assert.Equal(t, []byte{0x00, 0x00, 0x00, 0x00}, req.opaque)
	assert.Equal(t, []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, req.cas)
}

func TestProxyConnDecodeOk(t *testing.T) {
	ts := []struct {
		Name string
		Data []byte
		Err  error
		Key  []byte
		Cmd  byte
	}{
		// set cases
		{"SetOk", setTestData, nil, []byte("ABC"), byte(RequestTypeSet)},

		// Get Gets
		{"GetOk", setTestData, nil, []byte("ABC"), byte(RequestTypeGet)},

		// Delete
		{"DeleteOk", delTestData, nil, []byte("ABC"), byte(RequestTypeDelete)},

		// Incr/Decr
		{"IncrOk", incrTestData, nil, []byte("ABC"), byte(RequestTypeIncr)},

		// Touch
		{"TouchOk", touchTestData, nil, []byte("ABC"), byte(RequestTypeTouch)},

		// GetQ multi get
		{"MGetOk", getQTestData, nil, []byte("ABC"), byte(RequestTypeGetQ)},

		// Not support
		{"NotSupportCmd", notTestData, ErrBadRequest, []byte{}, 0xff},
		// {"NotFullLine", "baka 10", ErrBadRequest, "", ""},
	}

	for _, tt := range ts {
		t.Run(tt.Name, func(t *testing.T) {
			conn := libcon.NewConn(mockconn.CreateConn(tt.Data, 1), time.Second, time.Second)
			p := NewProxyConn(conn)
			mlist := proto.GetMsgs(2)

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
	getqResp := append(getQRespTestData[0], getQRespTestData[1]...)
	getqResp = append(getqResp, getQRespTestData[2]...)

	getqMissResp := append(getQRespTestData[0], getQMissRespTestData...)
	getqMissResp = append(getqMissResp, getQRespTestData[2]...)

	getAllMissResp := append(getQMissRespTestData, getQMissRespTestData...)
	getAllMissResp = append(getAllMissResp, getMissRespTestData...)

	ts := []struct {
		Name   string
		Req    []byte
		Resp   [][]byte
		Except []byte
	}{
		{Name: "SetOk", Req: setTestData, Resp: [][]byte{setRespTestData}, Except: setRespTestData},
		{Name: "GetOk", Req: getTestData, Resp: [][]byte{getRespTestData}, Except: getRespTestData},
		{Name: "GetMultiOk", Req: getQTestData,
			Resp:   getQRespTestData,
			Except: getqResp},

		{Name: "GetMultiMissOne", Req: getQTestData,
			Resp:   [][]byte{getQRespTestData[0], getQMissRespTestData, getQRespTestData[2]},
			Except: getqMissResp},

		{Name: "GetMultiAllMiss", Req: getQTestData,
			Resp:   [][]byte{getQMissRespTestData, getQMissRespTestData, getMissRespTestData},
			Except: getAllMissResp},
	}

	for _, tt := range ts {
		t.Run(tt.Name, func(t *testing.T) {
			conn := libcon.NewConn(mockconn.CreateConn(nil, 1), time.Second, time.Second)
			p := NewProxyConn(conn)
			msg := _createRespMsg(t, []byte(tt.Req), tt.Resp)
			err := p.Encode(msg)
			assert.NoError(t, err)
			assert.NoError(t, p.Flush())
			c := conn.Conn.(*mockconn.MockConn)
			buf := make([]byte, 1024)
			size, err := c.Wbuf.Read(buf)
			assert.NoError(t, err)
			assert.Equal(t, tt.Except, buf[:size])
		})
	}
}

func TestProxyConnEncodeErr(t *testing.T) {
	conn := libcon.NewConn(mockconn.CreateConn(nil, 1), time.Second, time.Second)
	p := NewProxyConn(conn)
	msg := proto.NewMessage()
	msg.WithRequest(&mockReq{})
	err := p.Encode(msg)
	_causeEqual(t, ErrAssertReq, err)

	msg = proto.NewMessage()
	msg.WithRequest(newReq())
	msg.WithError(errors.New("some error"))
	err = p.Encode(msg)
	assert.NoError(t, err)
	p.Flush()
	c := conn.Conn.(*mockconn.MockConn)
	buf := make([]byte, 1024)
	c.Wbuf.Read(buf)
	assert.Equal(t, resopnseStatusInternalErrBytes, buf[6:8])
}
