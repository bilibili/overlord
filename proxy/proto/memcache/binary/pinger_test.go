package binary

import (
	"testing"

	"overlord/pkg/bufio"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

func TestPingerPingOk(t *testing.T) {
	conn := _createConn(pongBs)
	pinger := NewPinger(conn)

	err := pinger.Ping()
	assert.NoError(t, err)
}

func TestPingerPingEOF(t *testing.T) {
	conn := _createRepeatConn(pongBs, 2)
	pinger := NewPinger(conn)

	err := pinger.Ping()
	assert.NoError(t, err)

	err = pinger.Ping()
	assert.NoError(t, err)
}

func TestPingerPing100Ok(t *testing.T) {
	conn := _createRepeatConn(pongBs, 100)
	pinger := NewPinger(conn)

	for i := 0; i < 100; i++ {
		err := pinger.Ping()
		assert.NoError(t, err, "error iter: %d", i)
	}

	err := pinger.Ping()
	assert.EqualError(t, err, "EOF")
}

func TestPingerFlushErr(t *testing.T) {
	conn := _createRepeatConn(pongBs, 100)
	c := conn.Conn.(*mockConn)
	c.err = errors.New("some error")
	pinger := NewPinger(conn)
	err := pinger.Ping()
	assert.EqualError(t, err, "some error")
}

func TestPingerClosed(t *testing.T) {
	conn := _createRepeatConn(pongBs, 100)
	pinger := NewPinger(conn)
	err := pinger.Close()
	assert.NoError(t, err)

	err = pinger.Ping()
	assert.Error(t, err)
	assert.NoError(t, pinger.Close())
}

func TestPingerNotReturnPong(t *testing.T) {
	conn := _createConn([]byte("iam test bytes 24 length"))
	pinger := NewPinger(conn)
	err := pinger.Ping()
	assert.Error(t, err)
	_causeEqual(t, ErrPingerPong, err)

	conn = _createConn([]byte("less than 24 length"))
	pinger = NewPinger(conn)
	err = pinger.Ping()
	assert.Error(t, err)
	_causeEqual(t, bufio.ErrBufferFull, err)
}
