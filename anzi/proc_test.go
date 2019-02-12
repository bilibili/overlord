package anzi

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetStrLenOk(t *testing.T) {
	assert.Equal(t, 2, getStrLen(10))
	assert.Equal(t, 3, getStrLen(999))
	assert.Equal(t, 5, getStrLen(99999))
	assert.Equal(t, 4, getStrLen(1024))
	assert.Equal(t, 4, getStrLen(1000))
}
