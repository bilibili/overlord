package memcache

import (
	"io"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

func TestPingerPingOk(t *testing.T) {
	conn := _createConn(pong)
	pinger := newMCPinger(conn)

	err := pinger.Ping()
	assert.NoError(t, err)
}

func TestPingerPingEOF(t *testing.T) {
	conn := _createConn(pong)
	pinger := newMCPinger(conn)

	err := pinger.Ping()
	assert.NoError(t, err)

	err = pinger.Ping()
	assert.Error(t, err)

	err = errors.Cause(err)
	assert.Equal(t, io.EOF, err)
}

func TestPingerPing100Ok(t *testing.T) {
	conn := _createRepeatConn(pong, 100)
	pinger := newMCPinger(conn)

	for i := 0; i < 100; i++ {
		err := pinger.Ping()
		assert.NoError(t, err)
	}

	err := pinger.Ping()
	assert.Error(t, err)
	_causeEqual(t, io.EOF, err)
}

func TestPingerClosed(t *testing.T) {
	conn := _createRepeatConn(pong, 100)
	pinger := newMCPinger(conn)
	err := pinger.Close()
	assert.NoError(t, err)

	err = pinger.Ping()
	assert.Error(t, err)
	assert.NoError(t, pinger.Close())
}

func TestPingerNotReturnPong(t *testing.T) {
	conn := _createRepeatConn([]byte("baka\r\n"), 100)
	pinger := newMCPinger(conn)
	err := pinger.Ping()
	assert.Error(t, err)
	_causeEqual(t, ErrPingerPong, err)
}
