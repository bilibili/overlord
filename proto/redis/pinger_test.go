package redis

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPingerPingOk(t *testing.T) {
	conn := _createRepeatConn(pongBytes, 10)
	p := newPinger(conn)
	err := p.ping()
	assert.NoError(t, err)
}

func TestPingerClosed(t *testing.T) {
	conn := _createRepeatConn(pongBytes, 10)
	p := newPinger(conn)
	assert.NoError(t, p.Close())
	err := p.ping()
	assert.Error(t, err)
	assert.EqualError(t, err, "ping interface has been closed")
	assert.NoError(t, p.Close())
}

func TestPingerWrongResp(t *testing.T) {
	conn := _createRepeatConn([]byte("-Error:badping\r\n"), 10)
	p := newPinger(conn)
	err := p.ping()
	assert.Error(t, err)
	assert.EqualError(t, err, "pong response payload is bad")
}
