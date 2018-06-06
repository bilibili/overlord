package redis

import (
	"bytes"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func _createRepeatConn(rdata []byte, count int) *connection {
	mconn := &mockConn{
		addr: "127.0.0.1:12345",
		rbuf: bytes.NewBuffer(bytes.Repeat(rdata, count)),
		wbuf: new(bytes.Buffer),
	}
	conn := newConn(mconn, time.Second, time.Second)
	return conn
}

func TestPingerPingOk(t *testing.T) {
	conn := _createRepeatConn([]byte("+PONG\r\n"), 10)
	pg := &pinger{
		addr:   "127.0.0.1:8000",
		conn:   conn,
		buf:    newBuffer(conn, conn),
		closed: pingerOpening,
	}

	for i := 0; i < 10; i++ {
		err := pg.Ping()
		assert.NoError(t, err)
	}

	err := pg.Ping()
	assert.Equal(t, io.EOF, err)
}

func TestPingerPingErrPingPong(t *testing.T) {
	conn := _createConn([]byte("+boynextdoor\r\n"))
	pg := &pinger{
		addr:   "127.0.0.1:8000",
		conn:   conn,
		buf:    newBuffer(conn, conn),
		closed: pingerOpening,
	}
	err := pg.Ping()
	assert.Error(t, err)
	assert.Equal(t, ErrPingPong, err)
}

func TestPingerPingErrEOF(t *testing.T) {
	conn := _createConn([]byte("boynextdoor\r\n"))
	pg := &pinger{
		addr:   "127.0.0.1:8000",
		conn:   conn,
		buf:    newBuffer(conn, conn),
		closed: pingerOpening,
	}
	err := pg.Ping()
	assert.Error(t, err)
	assert.Equal(t, ErrNotSupportRESPType, err)
}

func TestPingerNewFuncOk(t *testing.T) {
	pinger := NewPinger("127.0.0.1:12345", time.Second, time.Second, time.Second)
	assert.NotNil(t, pinger)
}
