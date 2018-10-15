package binary

import (
	"encoding/binary"
	"net"
	"testing"
	"time"

	"overlord/lib/bufio"
	"overlord/proto"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

func _createNodeConn(data []byte) *nodeConn {
	conn := _createConn(data)
	nc := &nodeConn{
		cluster: "clusterA",
		addr:    "127.0.0.1:5000",
		conn:    conn,
		bw:      bufio.NewWriter(conn),
		br:      bufio.NewReader(conn, bufio.Get(2048)),
	}
	return nc
}

func _createReqMsg(bin []byte) *proto.Message {
	mc := &MCRequest{}
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
	ts := []struct {
		name   string
		req    []byte
		except []byte
	}{
		{name: "get", req: getTestData, except: getTestData},
		{name: "set", req: setTestData, except: setTestData},
	}

	for _, tt := range ts {
		t.Run(tt.name, func(subt *testing.T) {
			req := _createReqMsg(tt.req)
			nc := _createNodeConn(nil)
			batch := proto.NewMsgBatch()
			batch.AddMsg(req)
			err := nc.WriteBatch(batch)
			assert.NoError(t, err)

			m, ok := nc.conn.Conn.(*mockConn)
			assert.True(t, ok)

			buf := make([]byte, 1024)
			size, err := m.wbuf.Read(buf)
			assert.NoError(t, err)
			assert.Equal(t, tt.except, buf[:size])
		})
	}
}

func TestNodeConnBatchWriteOk(t *testing.T) {
	ts := []struct {
		name   string
		req    []byte
		except []byte
	}{
		{name: "get", req: getTestData, except: getTestData},
		{name: "set", req: setTestData, except: setTestData},
	}

	for _, tt := range ts {
		t.Run(tt.name, func(subt *testing.T) {
			req := _createReqMsg(tt.req)
			nc := _createNodeConn(nil)
			batch := proto.NewMsgBatch()
			batch.AddMsg(req)

			err := nc.WriteBatch(batch)
			assert.NoError(t, err)
			err = nc.Flush()
			assert.NoError(t, err)

			m, ok := nc.conn.Conn.(*mockConn)
			assert.True(t, ok)

			buf := make([]byte, 1024)
			size, err := m.wbuf.Read(buf)
			assert.NoError(t, err)
			assert.Equal(t, tt.except, buf[:size])
		})
	}
}

func TestNodeConnWriteClosed(t *testing.T) {
	req := _createReqMsg(getTestData)
	nc := _createNodeConn(nil)
	err := nc.Close()
	assert.NoError(t, err)
	assert.True(t, nc.Closed())
	err = nc.write(req)
	assert.Error(t, err)
	_causeEqual(t, ErrClosed, err)
	assert.NoError(t, nc.Close())
	_causeEqual(t, ErrClosed, nc.Flush())
}

type mockReq struct {
}

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
	req := proto.NewMessage()
	nc := _createNodeConn(nil)
	req.WithRequest(&mockReq{})
	batch := proto.NewMsgBatch()
	batch.AddMsg(req)
	err := nc.WriteBatch(batch)
	assert.Error(t, err)
	err = nc.Flush()
	assert.NoError(t, err)
	_causeEqual(t, ErrAssertReq, err)
}

func TestNodeConnReadClosed(t *testing.T) {
	req := _createReqMsg(getTestData)
	nc := _createNodeConn(nil)

	err := nc.Close()
	assert.NoError(t, err)
	assert.True(t, nc.Closed())
	batch := proto.NewMsgBatch()
	batch.AddMsg(req)
	err = nc.ReadBatch(batch)
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
			req := _createReqMsg(tt.req)
			nc := _createNodeConn([]byte(tt.cData))
			batch := proto.NewMsgBatch()
			batch.AddMsg(req)
			err := nc.ReadBatch(batch)
			assert.NoError(t, err)

			mcr, ok := req.Request().(*MCRequest)
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

func TestNodeConnAssertError(t *testing.T) {
	nc := _createNodeConn(nil)
	req := proto.NewMessage()
	req.WithRequest(&mockReq{})
	batch := proto.NewMsgBatch()
	batch.AddMsg(req)
	err := nc.ReadBatch(batch)
	_causeEqual(t, ErrAssertReq, err)
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
