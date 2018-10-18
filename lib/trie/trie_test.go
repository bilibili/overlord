package trie

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTrieContainsOk(t *testing.T) {
	tt := New()
	tt.Add([]byte("abc"))
	tt.Add([]byte("ad"))
	tt.Add([]byte("ae"))
	tt.Add([]byte("abcde"))

	assert.False(t, tt.Contains([]byte("ab")))
	assert.True(t, tt.Contains([]byte("abc")))
	assert.True(t, tt.Contains([]byte("ad")))
	assert.True(t, tt.Contains([]byte("ae")))
	assert.True(t, tt.Contains([]byte("abcde")))
	assert.False(t, tt.Contains([]byte("abcd")))
}
