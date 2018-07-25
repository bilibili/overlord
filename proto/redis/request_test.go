package redis

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRequestNewRequest(t *testing.T) {
	cmd := NewRequest("GET", "a")
	assert.Equal(t, MergeTypeBasic, cmd.mergeType)
	assert.Equal(t, 2, cmd.respObj.Len())

	assert.Equal(t, "GET", cmd.CmdString())
	assert.Equal(t, "GET", string(cmd.Cmd()))
	assert.Equal(t, "a", string(cmd.Key()))
}
