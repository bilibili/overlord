package binary

import (
	"overlord/pkg/mockconn"
	"testing"
	"time"

	"overlord/pkg/bufio"
	libcon "overlord/pkg/net"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

func TestPingerPingOk(t *testing.T) {
	conn := libcon.NewConn(mockconn.CreateConn(pongBs, 1), time.Second, time.Second)
	pinger := NewPinger(conn)

	err := pinger.Ping()
	assert.NoError(t, err)
}

func TestPingerPingEOF(t *testing.T) {
	conn := libcon.NewConn(mockconn.CreateConn(pongBs, 2), time.Second, time.Second)
	pinger := NewPinger(conn)

	err := pinger.Ping()
	assert.NoError(t, err)

	err = pinger.Ping()
	assert.NoError(t, err)
}

func TestPingerPing100Ok(t *testing.T) {
	conn := libcon.NewConn(mockconn.CreateConn(pongBs, 100), time.Second, time.Second)
	pinger := NewPinger(conn)

	for i := 0; i < 100; i++ {
		err := pinger.Ping()
		assert.NoError(t, err, "error iter: %d", i)
	}

	err := pinger.Ping()
	assert.EqualError(t, err, "EOF")
}

func TestPingerFlushErr(t *testing.T) {
	conn := libcon.NewConn(mockconn.CreateConn(pongBs, 100), time.Second, time.Second)
	c := conn.Conn.(*mockconn.MockConn)
	c.Err = errors.New("some error")
	pinger := NewPinger(conn)
	err := pinger.Ping()
	assert.EqualError(t, err, "some error")
}

func TestPingerClosed(t *testing.T) {
	conn := libcon.NewConn(mockconn.CreateConn(pongBs, 100), time.Second, time.Second)
	pinger := NewPinger(conn)
	err := pinger.Close()
	assert.NoError(t, err)

	err = pinger.Ping()
	assert.Error(t, err)
	assert.NoError(t, pinger.Close())
}

func TestPingerNotReturnPong(t *testing.T) {
	conn := libcon.NewConn(mockconn.CreateConn([]byte("iam test bytes 24 length"), 1), time.Second, time.Second)
	pinger := NewPinger(conn)
	err := pinger.Ping()
	assert.Error(t, err)
	_causeEqual(t, ErrPingerPong, err)

	conn = libcon.NewConn(mockconn.CreateConn([]byte("less than 24 length"), 1), time.Second, time.Second)
	pinger = NewPinger(conn)
	err = pinger.Ping()
	assert.Error(t, err)
	_causeEqual(t, bufio.ErrBufferFull, err)
}
