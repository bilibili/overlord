package conv

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBtoi(t *testing.T) {
	i := int64(123)
	is := []byte("123")
	ni, err := Btoi(is)
	assert.NoError(t, err)
	assert.Equal(t, i, ni)
}

func TestUpdateToLower(t *testing.T) {
	bs := []byte{'A', 'B', 'c'}
	UpdateToLower(bs)
	assert.Equal(t, []byte{'a', 'b', 'c'}, bs)
}

func TestUpdateToUpper(t *testing.T) {
	bs := []byte{'a', 'b', 'C'}
	UpdateToUpper(bs)
	assert.Equal(t, []byte{'A', 'B', 'C'}, bs)
}
