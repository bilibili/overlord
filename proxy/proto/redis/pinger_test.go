package redis

import (
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

func TestPingerPingOk(t *testing.T) {
	conn := _createConn(pongBytes)
	p := NewPinger(conn)
	err := p.Ping()
	assert.NoError(t, err)
}

func TestPingerClosed(t *testing.T) {
	conn := _createRepeatConn(pongBytes, 10)
	p := NewPinger(conn)
	assert.NoError(t, p.Close())
	err := p.Ping()
	assert.Equal(t, ErrPingClosed, errors.Cause(err))
	assert.NoError(t, p.Close())
}

func TestPingerWrongResp(t *testing.T) {
	conn := _createConn([]byte("-Error: iam more than 7 bytes\r\n"))
	p := NewPinger(conn)
	err := p.Ping()
	assert.Equal(t, ErrBadPong, errors.Cause(err))

	conn = _createConn([]byte("-Err\r\n"))
	p = NewPinger(conn)
	err = p.Ping()
	assert.Equal(t, ErrBadPong, errors.Cause(err))
}

func TestPingerPingErr(t *testing.T) {
	conn := _createConn(pongBytes)
	c := conn.Conn.(*mockConn)
	c.err = errors.New("some error")
	p := NewPinger(conn)
	err := p.Ping()
	assert.EqualError(t, err, "some error")
}
