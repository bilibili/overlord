package redis

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseRedirectInfoWithTooFewFields(t *testing.T) {
	_, err := parseRedirectInfo([]byte("MOVED 127.0.0.1 7001 123"))
	assert.Error(t, err)
	assert.Equal(t, ErrRedirectBadFormat, err)
}

func TestParseRedirectInfoOk(t *testing.T) {
	ri, err := parseRedirectInfo([]byte("MOVED 7001 127.0.0.1:2777"))
	assert.NoError(t, err)
	assert.Equal(t, 7001, ri.Slot)
	assert.Equal(t, "127.0.0.1:2777", ri.Addr)
	assert.False(t, ri.IsAsk)
}

func TestParseRedirectInfoWithWrongSlotNumber(t *testing.T) {
	_, err := parseRedirectInfo([]byte("MOVED abcde 127.0.0.1:2777"))
	assert.Error(t, err)
}

func TestIsRedirectOk(t *testing.T) {
	robj := newresp(respError, []byte("MOVED 7000 127.0.0.1:2154"))
	assert.True(t, isRedirect(robj))

	robj = newresp(respString, []byte("MOVED 7000 127.0.0.1:1231"))
	assert.False(t, isRedirect(robj))

	robj = newresp(respError, []byte("..0.1"))
	assert.False(t, isRedirect(robj))

	robj = newresp(respError, []byte("ASK 1888 127.0.0.1:19243"))
	assert.True(t, isRedirect(robj))

	robj = newresp(respError, nil)
	assert.False(t, isRedirect(robj))
}
