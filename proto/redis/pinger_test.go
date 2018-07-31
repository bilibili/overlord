package redis

import (
	"testing"

	"overlord/lib/bufio"

	"github.com/stretchr/testify/assert"
)

func TestPingerPingOk(t *testing.T) {
	conn := _createConn(pongBytes)
	p := newPinger(conn)
	err := p.ping()
	assert.NoError(t, err)
}

func TestPingerClosed(t *testing.T) {
	conn := _createRepeatConn(pongBytes, 10)
	p := newPinger(conn)
	assert.NoError(t, p.Close())
	err := p.ping()
	assert.Equal(t, ErrPingClosed, err)
	assert.NoError(t, p.Close())
}

func TestPingerWrongResp(t *testing.T) {
	conn := _createConn([]byte("-Error: iam more than 7 bytes\r\n"))
	p := newPinger(conn)
	err := p.ping()
	assert.Equal(t, bufio.ErrBufferFull, err)

	conn = _createConn([]byte("-Err\r\n"))
	p = newPinger(conn)
	err = p.ping()
	assert.Equal(t, ErrBadPong, err)
}
