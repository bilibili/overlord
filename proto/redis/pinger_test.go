package redis

import (
	"testing"

	"overlord/lib/bufio"

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
	assert.Equal(t, ErrPingClosed, err)
	assert.NoError(t, p.Close())
}

func TestPingerWrongResp(t *testing.T) {
	conn := _createConn([]byte("-Error: iam more than 7 bytes\r\n"))
	p := NewPinger(conn)
	err := p.Ping()
	assert.Equal(t, bufio.ErrBufferFull, err)

	conn = _createConn([]byte("-Err\r\n"))
	p = NewPinger(conn)
	err = p.Ping()
	assert.Equal(t, ErrBadPong, err)
}
