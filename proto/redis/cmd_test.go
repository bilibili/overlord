package redis

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCommandNewCommand(t *testing.T) {
	cmd := NewCommand("GET", "a")
	assert.Equal(t, MergeTypeBasic, cmd.mergeType)
	assert.Equal(t, 2, cmd.respObj.Len())

	assert.Equal(t, "GET", cmd.CmdString())
	assert.Equal(t, "GET", string(cmd.Cmd()))
	assert.Equal(t, "a", string(cmd.Key()))
}

func TestCommandRedirect(t *testing.T) {
	cmd := NewCommand("GET", "BAKA")
	cmd.reply = newRespPlain(respError, []byte("ASK 1024 127.0.0.1:2048"))
	assert.True(t, cmd.IsRedirect())
	r, slot, addr, err := cmd.RedirectTriple()
	assert.NoError(t, err)
	assert.Equal(t, "ASK", r)
	assert.Equal(t, 1024, slot)
	assert.Equal(t, "127.0.0.1:2048", addr)
}