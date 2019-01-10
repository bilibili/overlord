package bufio

import (
	"bytes"
	"errors"
	"net"
	"testing"
	"time"

	libnet "overlord/pkg/net"

	"github.com/stretchr/testify/assert"
)

const fbyte = byte('f')

func _genData() []byte {
	bts := bytes.Repeat([]byte("abcde"), 3*100)
	bts[len(bts)-1] = fbyte
	return bts
}

func TestReaderAdvance(t *testing.T) {
	bts := _genData()

	b := NewReader(bytes.NewBuffer(bts), Get(defaultBufferSize))
	b.Read()
	b.ReadExact(5)
	b.Advance(5)

	buf := b.Buffer()
	assert.NotNil(t, buf)

	assert.Len(t, buf.Bytes(), 502)
	b.Advance(-10)
	assert.Len(t, buf.Bytes(), 512)

	b.ReadExact(10)
	m := b.Mark()
	assert.Equal(t, 10, m)

	b.AdvanceTo(5)
	m = b.Mark()
	assert.Equal(t, 5, m)
	assert.Equal(t, 5, b.Buffer().r)
}

func TestReaderRead(t *testing.T) {
	bts := _genData()

	b := NewReader(bytes.NewBuffer(bts), Get(defaultBufferSize))
	err := b.Read()
	assert.NoError(t, err)

	b.err = errors.New("some error")
	err = b.Read()
	assert.EqualError(t, err, "some error")
}

func TestReaderReadSlice(t *testing.T) {
	bts := _genData()

	b := NewReader(bytes.NewBuffer(bts), Get(defaultBufferSize))
	b.Read()
	data, err := b.ReadSlice('c')
	assert.NoError(t, err)
	assert.Len(t, data, 3)

	_, err = b.ReadSlice('\n')
	assert.Equal(t, ErrBufferFull, err)
}

func TestReaderReadLine(t *testing.T) {
	b := NewReader(bytes.NewBuffer([]byte("abcd\r\nabc")), Get(defaultBufferSize))
	b.Read()
	data, err := b.ReadLine()
	assert.NoError(t, err)
	assert.Len(t, data, 6)
}

func TestReaderReadExact(t *testing.T) {
	bts := _genData()

	b := NewReader(bytes.NewBuffer(bts), Get(defaultBufferSize))
	b.Read()
	data, err := b.ReadExact(5)
	assert.NoError(t, err)
	assert.Len(t, data, 5)

	_, err = b.ReadExact(5 * 3 * 100)
	assert.Equal(t, ErrBufferFull, err)
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

	err := w.Flush()
	assert.NoError(t, err)

	err = w.Write([]byte(data))
	assert.NoError(t, err)

	err = w.Write([]byte(data))
	assert.NoError(t, err)

	err = w.Flush()
	assert.NoError(t, err)

	w.err = errors.New("some error")
	err = w.Write([]byte(data))
	assert.EqualError(t, err, "some error")
	err = w.Flush()
	assert.EqualError(t, err, "some error")
}
