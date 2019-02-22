package mockconn

import (
	"bytes"
	"io"
	"net"
	"sync/atomic"
	"time"
)

const (
	stateClosed  = 1
	stateOpening = 0
)

type mockAddr string

func (m mockAddr) Network() string {
	return "tcp"
}
func (m mockAddr) String() string {
	return string(m)
}

// MockConn mock tcp conn.
type MockConn struct {
	addr   mockAddr
	rbuf   *bytes.Buffer
	Wbuf   *bytes.Buffer
	data   []byte
	repeat int
	Err    error
	closed int32
}

func (m *MockConn) Read(b []byte) (n int, err error) {
	if atomic.LoadInt32(&m.closed) == stateClosed {
		return 0, io.EOF
	}
	if m.Err != nil {
		err = m.Err
		return
	}
	if m.repeat > 0 {
		m.rbuf.Write(m.data)
		m.repeat--
	}
	return m.rbuf.Read(b)
}

func (m *MockConn) Write(b []byte) (n int, err error) {
	if atomic.LoadInt32(&m.closed) == stateClosed {
		return 0, io.EOF
	}

	if m.Err != nil {
		err = m.Err
		return
	}
	return m.Wbuf.Write(b)
}

// writeBuffers impl the net.buffersWriter to support writev
func (m *MockConn) writeBuffers(buf *net.Buffers) (int64, error) {
	if m.Err != nil {
		return 0, m.Err
	}
	return buf.WriteTo(m.Wbuf)
}

func (m *MockConn) Close() error {
	atomic.StoreInt32(&m.closed, stateClosed)
	return nil
}

func (m *MockConn) LocalAddr() net.Addr  { return m.addr }
func (m *MockConn) RemoteAddr() net.Addr { return m.addr }

func (m *MockConn) SetDeadline(t time.Time) error      { return nil }
func (m *MockConn) SetReadDeadline(t time.Time) error  { return nil }
func (m *MockConn) SetWriteDeadline(t time.Time) error { return nil }

// CreateConn with mock data repeate for r times.
func CreateConn(data []byte, r int) net.Conn {
	mconn := &MockConn{
		addr:   "127.0.0.1:12345",
		rbuf:   bytes.NewBuffer(nil),
		Wbuf:   new(bytes.Buffer),
		data:   data,
		repeat: r,
	}
	return mconn
}

// CreateDownStreamConn for mock conn write.
func CreateDownStreamConn() (net.Conn, *bytes.Buffer) {
	buf := new(bytes.Buffer)
	mconn := &MockConn{
		addr: "127.0.0.1:12345",
		Wbuf: buf,
	}
	return mconn, buf
}
