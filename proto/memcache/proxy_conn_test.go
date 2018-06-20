package memcache

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFindLengthGetsResponseOk(t *testing.T) {
	x := []byte{86, 65, 76, 85, 69, 32, 97, 95, 49, 49, 32, 48, 32, 49, 32, 51, 56, 51, 55, 13, 10}
	i, err := findLength(x, true)
	assert.NoError(t, err)
	assert.Equal(t, 1, i)

	i, err = findLength(x, false)
	assert.NoError(t, err)
	assert.Equal(t, 3837, i)
}
