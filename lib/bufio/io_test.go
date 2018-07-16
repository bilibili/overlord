package bufio

import (
	"bytes"
	"io"
	"net"
	libnet "overlord/lib/net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

const fbyte = byte('f')

func _genData() []byte {
	bts := bytes.Repeat([]byte("abcde"), 3*100)
	bts[len(bts)-1] = fbyte
	return bts
}

func TestReaderRead(t *testing.T) {
	bts := _genData()

	b := NewReader(bytes.NewBuffer(bts), Get(defaultBufferSize))
	err := b.Read()
	assert.NoError(t, err)
}

func TestReaderReadUntil(t *testing.T) {
	bts := _genData()
	b := NewReader(bytes.NewBuffer(bts), Get(defaultBufferSize))
	data, err := b.ReadUntil(fbyte)
	assert.NoError(t, err)
	assert.Len(t, data, 5*3*100)
}

func TestReaderReadSlice(t *testing.T) {
	bts := _genData()

	b := NewReader(bytes.NewBuffer(bts), Get(defaultBufferSize))
	b.Read()
	data, err := b.ReadSlice('c')
	assert.NoError(t, err)
	assert.Len(t, data, 3)
}

func TestReaderReadFull(t *testing.T) {
	bts := _genData()

	b := NewReader(bytes.NewBuffer(bts), Get(defaultBufferSize))
	data, err := b.ReadFull(1200)
	assert.NoError(t, err)
	assert.Len(t, data, 1200)
}

func TestReaderReadExact(t *testing.T) {
	bts := _genData()

	b := NewReader(bytes.NewBuffer(bts), Get(defaultBufferSize))
	b.Read()
	data, err := b.ReadExact(5)
	assert.NoError(t, err)
	assert.Len(t, data, 5)
}

func TestReaderResetBuffer(t *testing.T) {
	bts := _genData()
	b := NewReader(bytes.NewBuffer(bts), Get(defaultBufferSize))

	_, err := b.ReadFull(1200)
	assert.NoError(t, err)

	b.ResetBuffer(Get(defaultBufferSize))
	data, err := b.ReadFull(300)
	assert.NoError(t, err)
	assert.Len(t, data, 300)

	_, err = b.ReadFull(300)
	assert.Error(t, err)
	assert.Equal(t, io.EOF, err)
}

type mockAddr string

func (m mockAddr) Network() string {
	return "tcp"
}
func (m mockAddr) String() string {
	return string(m)
}

type mockConn struct {
	rbuf *bytes.Buffer
	wbuf *bytes.Buffer
	addr mockAddr
}

func (m *mockConn) Read(b []byte) (n int, err error) {
	return m.rbuf.Read(b)
}
func (m *mockConn) Write(b []byte) (n int, err error) {
	return m.wbuf.Write(b)
}

// writeBuffers impl the net.buffersWriter to support writev
func (m *mockConn) writeBuffers(buf *net.Buffers) (int64, error) {
	return buf.WriteTo(m.wbuf)
}

func (m *mockConn) Close() error         { return nil }
func (m *mockConn) LocalAddr() net.Addr  { return m.addr }
func (m *mockConn) RemoteAddr() net.Addr { return m.addr }

func (m *mockConn) SetDeadline(t time.Time) error      { return nil }
func (m *mockConn) SetReadDeadline(t time.Time) error  { return nil }
func (m *mockConn) SetWriteDeadline(t time.Time) error { return nil }

// _createConn is useful tools for handler test
func _createConn(data []byte) *libnet.Conn {
	return _createRepeatConn(data, 1)
}

func _createRepeatConn(data []byte, r int) *libnet.Conn {
	mconn := &mockConn{
		addr: "127.0.0.1:12345",
		rbuf: bytes.NewBuffer(bytes.Repeat(data, r)),
		wbuf: new(bytes.Buffer),
	}
	conn := libnet.NewConn(mconn, time.Second, time.Second)
	return conn
}

func TestWriterWriteOk(t *testing.T) {
	data := "Bilibili 干杯 - ( ゜- ゜)つロ"

	conn := _createConn(nil)
	w := NewWriter(conn)
	err := w.Write([]byte(data))
	assert.NoError(t, err)

	err = w.Write([]byte(data))
	assert.NoError(t, err)

	err = w.Flush()
	assert.NoError(t, err)
}
