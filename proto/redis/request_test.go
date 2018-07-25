package redis

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRequestNewRequest(t *testing.T) {
	cmd := NewRequest("GET", "a")
	assert.Equal(t, MergeTypeJoin, cmd.mergeType)
	assert.Equal(t, 2, cmd.respObj.Len())

	assert.Equal(t, "3\r\nGET", cmd.CmdString())
	assert.Equal(t, "3\r\nGET", string(cmd.Cmd()))
	assert.Equal(t, "a", string(cmd.Key()))
}

func TestRequestRedirect(t *testing.T) {
	cmd := NewRequest("GET", "BAKA")
	cmd.reply = newRESPPlain(respError, []byte("ASK 1024 127.0.0.1:2048"))
	assert.True(t, cmd.IsRedirect())
	r, slot, addr, err := cmd.RedirectTriple()
	assert.NoError(t, err)
	assert.Equal(t, "ASK", r)
	assert.Equal(t, 1024, slot)
	assert.Equal(t, "127.0.0.1:2048", addr)
	cmd.reply = newRESPPlain(respError, []byte("ERROR"))
	assert.False(t, cmd.IsRedirect())
	_, _, _, err = cmd.RedirectTriple()
	assert.Error(t, err)
}
