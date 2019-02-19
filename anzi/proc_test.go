package anzi

import (
	"io"
	"testing"
	"time"

	"bufio"
	"bytes"

	"github.com/stretchr/testify/assert"
)

func TestGetStrLenOk(t *testing.T) {
	assert.Equal(t, 2, getStrLen(10))
	assert.Equal(t, 3, getStrLen(999))
	assert.Equal(t, 5, getStrLen(99999))
	assert.Equal(t, 4, getStrLen(1024))
	assert.Equal(t, 4, getStrLen(1000))
}

func TestSyncRDBCmdOk(t *testing.T) {
	buf := bytes.NewBuffer(nil)

	inst := &Instance{
		br:     bufio.NewReader(buf),
		bw:     bufio.NewWriter(buf),
		offset: int64(120),
	}
	inst.replAckConf()
	data := make([]byte, 36)
	size, err := io.ReadFull(buf, data)
	assert.Equal(t, 36, size)
	assert.NoError(t, err)
}

var longData = `"I am the Bone of my Sword
Steel is my Body and Fire is my Blood.
I have created over a Thousand Blades,
Unknown to Death,
Nor known to Life.
Have withstood Pain to create many Weapons
Yet those Hands will never hold Anything.
So, as I Pray--
Unlimited Blade Works"`

func TestWriteAllOk(t *testing.T) {
	buf := bytes.NewBuffer(nil)
	err := writeAll([]byte(longData), buf)
	assert.NoError(t, err)
	assert.Equal(t, len(longData), buf.Len())
}

func TestCmdForwrad(t *testing.T) {
	tconn := _createConn([]byte(longData))
	conn := _createRepeatConn([]byte(longData), 10)
	inst := &Instance{
		tconn: tconn,

		conn: conn,
		br:   bufio.NewReader(conn),
		bw:   bufio.NewWriter(conn),
	}
	go inst.cmdForward()
	time.Sleep(time.Millisecond * 10)
	inst.Close()
}

func TestParsePSyncReply(t *testing.T) {
	data := []byte("+fullsync 0123456789012345678901234567890123456789 7788\r\n")
	inst := &Instance{}
	err := inst.parsePSyncReply(data)
	assert.NoError(t, err)
	assert.Equal(t, "0123456789012345678901234567890123456789", inst.masterID)
	assert.Equal(t, int64(7788), inst.offset)
}
