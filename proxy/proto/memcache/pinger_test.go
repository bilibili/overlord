package memcache

import (
	"overlord/pkg/mockconn"
	libnet "overlord/pkg/net"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

func TestPingerPingOk(t *testing.T) {
	conn := libnet.NewConn(mockconn.CreateConn(pongBytes, 1), time.Second, time.Second)
	pinger := NewPinger(conn)

	err := pinger.Ping()
	assert.NoError(t, err)
}

func TestPingerPingMore(t *testing.T) {
	conn := libnet.NewConn(mockconn.CreateConn(pongBytes, 2), time.Second, time.Second)
	pinger := NewPinger(conn)

	err := pinger.Ping()
	assert.NoError(t, err)

	err = pinger.Ping()
	assert.NoError(t, err)
}

func TestPingerPing100Ok(t *testing.T) {
	conn := libnet.NewConn(mockconn.CreateConn(pongBytes, 100), time.Second, time.Second)
	pinger := NewPinger(conn)

	for i := 0; i < 100; i++ {
		err := pinger.Ping()
		assert.NoError(t, err, "error iter: %d", i)
	}

	err := pinger.Ping()
	assert.EqualError(t, err, "EOF")
}

func TestPingerErr(t *testing.T) {
	conn := libnet.NewConn(mockconn.CreateConn(pongBytes, 100), time.Second, time.Second)
	c := conn.Conn.(*mockconn.MockConn)
	c.Err = errors.New("some error")
	pinger := NewPinger(conn)
	err := pinger.Ping()
	assert.EqualError(t, err, "some error")
}

func TestPingerClosed(t *testing.T) {
	conn := libnet.NewConn(mockconn.CreateConn(pongBytes, 100), time.Second, time.Second)
	pinger := NewPinger(conn)
	err := pinger.Close()
	assert.NoError(t, err)

	err = pinger.Ping()
	assert.Error(t, err)
	assert.NoError(t, pinger.Close())
}

func TestPingerNotReturnPong(t *testing.T) {
	conn := libnet.NewConn(mockconn.CreateConn([]byte("baka\r\n"), 100), time.Second, time.Second)
	pinger := NewPinger(conn)
	err := pinger.Ping()
	assert.Error(t, err)
	_causeEqual(t, ErrPingerPong, err)
}

func TestPingerFinishBufferOffset(t *testing.T) {
	conn := libnet.NewConn(mockconn.CreateConn(pongBytes, 100), time.Second, time.Second)
	pinger := NewPinger(conn)
	err := pinger.Ping()
	assert.NoError(t, err, "there is no error")

	p, ok := pinger.(*mcPinger)
	assert.True(t, ok)
	buf := p.br.Buffer()
	assert.Len(t, buf.Bytes(), 0)
}
