package binary

import (
	"encoding/binary"
	"net"
	"testing"
	"time"

	"overlord/pkg/bufio"
	"overlord/proxy/proto"

	"overlord/pkg/mockconn"
	libnet "overlord/pkg/net"

	"github.com/pkg/errors"

	"github.com/stretchr/testify/assert"
)

func _createNodeConn(data []byte) *nodeConn {
	conn := libnet.NewConn(mockconn.CreateConn(data, 1), time.Second, time.Second)
	nc := &nodeConn{
		cluster: "clusterA",
		addr:    "127.0.0.1:5000",
		conn:    conn,
		bw:      bufio.NewWriter(conn),
		br:      bufio.NewReader(conn, bufio.Get(1024)),
	}
	return nc
}

func _createReqMsg(bin []byte) *proto.Message {
	mc := newReq()
	parseHeader(bin, mc, true)

	bl := int(binary.BigEndian.Uint32(mc.bodyLen))
	el := int(uint8(mc.extraLen[0]))
	kl := int(binary.BigEndian.Uint16(mc.keyLen))
	if kl > 0 {
		mc.key = bin[24+el : 24+el+kl]
	}
	if bl > 0 {
		mc.data = bin[24:]
	}
	pm := proto.NewMessage()
	pm.WithRequest(mc)
	return pm
}

func _causeEqual(t *testing.T, except, actual error) {
	err := errors.Cause(actual)
	assert.Equal(t, except, err)
}

func TestNodeConnWriteOk(t *testing.T) {
	var (
		getQBs = []byte{
			0x80,       // magic
			0x0d,       // cmd // NOTE: cmd will change
			0x00, 0x03, // key len
			0x00,       // extra len
			0x00,       // data type
			0x00, 0x00, // vbucket
			0x00, 0x00, 0x00, 0x03, // body len
			0x00, 0x00, 0x00, 0x00, // opaque
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // cas
			0x41, 0x42, 0x43, // key: ABC
		}
		getQRespBs = []byte{
			0x80,       // magic
			0x0c,       // cmd // NOTE: cmd changed
			0x00, 0x03, // key len
			0x00,       // extra len
			0x00,       // data type
			0x00, 0x00, // vbucket
			0x00, 0x00, 0x00, 0x03, // body len
			0x00, 0x00, 0x00, 0x00, // opaque
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // cas
			0x41, 0x42, 0x43, // key: ABC
		}
	)

	ts := []struct {
		name   string
		req    []byte
		except []byte
	}{
		{name: "get", req: getTestData, except: getTestData},
		{name: "getq", req: getQBs, except: getQRespBs},
		{name: "set", req: setTestData, except: setTestData},
	}

	for _, tt := range ts {
		t.Run(tt.name, func(subt *testing.T) {
			msg := _createReqMsg(tt.req)
			nc := _createNodeConn(nil)

			err := nc.Write(msg)
			assert.NoError(t, err)
			err = nc.Flush()
			assert.NoError(t, err)

			m, ok := nc.conn.Conn.(*mockconn.MockConn)
			assert.True(t, ok)

			buf := make([]byte, 1024)
			size, err := m.Wbuf.Read(buf)
			assert.NoError(t, err)
			assert.Equal(t, tt.except, buf[:size])
		})
	}
}

func TestNodeConnWriteClosed(t *testing.T) {
	msg := _createReqMsg(getTestData)
	nc := _createNodeConn(nil)
	err := nc.Close()
	assert.NoError(t, err)
	assert.True(t, nc.Closed())
	err = nc.Write(msg)
	assert.Error(t, err)
	_causeEqual(t, ErrClosed, err)
	err = nc.Flush()
	assert.Error(t, err)
	_causeEqual(t, ErrClosed, err)
	assert.NoError(t, nc.Close())
	_causeEqual(t, ErrClosed, nc.Write(nil))
}

type mockReq struct {
}

func (*mockReq) Slowlog() *proto.SlowlogEntry { return nil }

func (*mockReq) CmdString() string {
	return ""
}

func (*mockReq) Cmd() []byte {
	return []byte("")
}

func (*mockReq) Key() []byte {
	return []byte{}
}

func (*mockReq) Resp() []byte {
	return nil
}

func (*mockReq) Put() {

}
func TestNodeConnWriteTypeAssertFail(t *testing.T) {
	msg := proto.NewMessage()
	nc := _createNodeConn(nil)
	msg.WithRequest(&mockReq{})

	err := nc.Write(msg)
	nc.Flush()
	assert.Error(t, err)
	_causeEqual(t, ErrAssertReq, errors.Cause(err))
}

func TestNodeConnReadClosed(t *testing.T) {
	msg := _createReqMsg(getTestData)
	nc := _createNodeConn(nil)

	err := nc.Close()
	assert.NoError(t, err)
	assert.True(t, nc.Closed())

	err = nc.Read(msg)
	assert.Error(t, err)
	_causeEqual(t, ErrClosed, err)
}

func TestNodeConnReadOk(t *testing.T) {
	ts := []struct {
		name   string
		req    []byte
		cData  []byte
		except []byte
	}{
		{
			name:  "getmiss",
			req:   getTestData,
			cData: getMissRespTestData, except: getMissRespTestData,
		},
		{
			name:  "get ok",
			req:   getTestData,
			cData: getRespTestData, except: getRespTestData,
		},
		{
			name:  "set ok",
			req:   setTestData,
			cData: setRespTestData, except: setRespTestData,
		},
	}
	for _, tt := range ts {

		t.Run(tt.name, func(t *testing.T) {
			msg := _createReqMsg(tt.req)
			nc := _createNodeConn([]byte(tt.cData))

			err := nc.Read(msg)
			assert.NoError(t, err)

			mcr, ok := msg.Request().(*MCRequest)
			assert.Equal(t, true, ok)

			actual := append([]byte{mcr.magic}, mcr.rTp.Bytes()...)
			actual = append(actual, mcr.keyLen...)
			actual = append(actual, mcr.extraLen...)
			actual = append(actual, zeroBytes...)  // datatype
			actual = append(actual, mcr.status...) // status
			actual = append(actual, mcr.bodyLen...)
			actual = append(actual, mcr.opaque...)
			actual = append(actual, mcr.cas...)
			bl := binary.BigEndian.Uint32(mcr.bodyLen)
			if bl > 0 {
				actual = append(actual, mcr.data...)
			}

			assert.Equal(t, tt.except, actual)
		})

	}
}

func TestNodeConnError(t *testing.T) {
	nc := _createNodeConn(nil)
	msg := proto.NewMessage()
	msg.WithRequest(&mockReq{})

	err := nc.Read(msg)
	_causeEqual(t, ErrAssertReq, err)

	notLenBs := []byte{
		0x80,       // magic
		0x0c,       // cmd
		0x00, 0x00, // key len
		0x00,       // extra len
		0x00,       // data type
		0x00, 0x00, // vbucket
		0x00, 0x00, 0x00, 0x00, // body len
		0x00, 0x00, 0x00, 0x00, // opaque
		// 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // cas
		// 0x41, 0x42, 0x43, // key: ABC
	}
	msg = proto.NewMessage()
	req := newReq()
	msg.WithRequest(req)
	nc = _createNodeConn(notLenBs)
	err = nc.Read(msg)
	assert.EqualError(t, err, "EOF")

	notLenBs = []byte{
		0x80,       // magic
		0x0c,       // cmd
		0x00, 0x03, // key len
		0x00,       // extra len
		0x00,       // data type
		0x00, 0x00, // vbucket
		0x00, 0x00, 0x00, 0x06, // body len
		0x00, 0x00, 0x00, 0x00, // opaque
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // cas
		0x41, 0x42, 0x43, // key: ABC
	}
	msg = proto.NewMessage()
	req = newReq()
	msg.WithRequest(req)
	nc = _createNodeConn(notLenBs)
	err = nc.Read(msg)
	assert.EqualError(t, err, "EOF")
}

func TestNewNodeConnWithClosedBinder(t *testing.T) {
	taddr, err := net.ResolveTCPAddr("tcp", "127.0.0.1:0")
	assert.NoError(t, err)
	listener, err := net.ListenTCP("tcp", taddr)
	assert.NoError(t, err)
	addr := listener.Addr()
	go func() {
		defer listener.Close()
		sock, _ := listener.Accept()
		defer sock.Close()
	}()
	nc := NewNodeConn("anyName", addr.String(), time.Second, time.Second, time.Second)
	assert.NotNil(t, nc)
}
