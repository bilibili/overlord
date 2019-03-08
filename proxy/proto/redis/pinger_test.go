package redis

import (
	"testing"
	"time"

	"overlord/pkg/mockconn"
	libnet "overlord/pkg/net"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

func TestPingerPingOk(t *testing.T) {
	conn := libnet.NewConn(mockconn.CreateConn(pongBytes, 1), time.Second, time.Second)
	p := NewPinger(conn)
	err := p.Ping()
	assert.NoError(t, err)
}

func TestPingerClosed(t *testing.T) {
	conn := libnet.NewConn(mockconn.CreateConn(pongBytes, 10), time.Second, time.Second)
	p := NewPinger(conn)
	assert.NoError(t, p.Close())
	err := p.Ping()
	assert.Equal(t, ErrPingClosed, errors.Cause(err))
	assert.NoError(t, p.Close())
}

func TestPingerWrongResp(t *testing.T) {
	conn := libnet.NewConn(mockconn.CreateConn([]byte("-Error: iam more than 7 bytes\r\n"), 1), time.Second, time.Second)
	p := NewPinger(conn)
	err := p.Ping()
	assert.Equal(t, ErrBadPong, errors.Cause(err))
	conn = libnet.NewConn(mockconn.CreateConn([]byte("-Err\r\n"), 1), time.Second, time.Second)
	p = NewPinger(conn)
	err = p.Ping()
	assert.Equal(t, ErrBadPong, errors.Cause(err))
}

func TestPingerPingErr(t *testing.T) {
	conn := libnet.NewConn(mockconn.CreateConn(pingBytes, 1), time.Second, time.Second)
	c := conn.Conn.(*mockconn.MockConn)
	c.Err = errors.New("some error")
	p := NewPinger(conn)
	err := p.Ping()
	assert.EqualError(t, err, "some error")
}
