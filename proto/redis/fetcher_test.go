package redis

import (
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

var _exampleData = "$5\r\nabcde\r\n"

func TestFetcherFetchOk(t *testing.T) {
	conn := _createRepeatConn([]byte(_exampleData), 10)
	fetcher := NewFetcher(conn)
	data, err := fetcher.Fetch()
	if assert.NoError(t, err) {
		assert.Equal(t, []byte("abcde"), data)
		assert.NoError(t, fetcher.Close())
	}
}

func TestFetcherWithBadResponse(t *testing.T) {
	conn := _createRepeatConn([]byte("+deep dark fantasy\r\n"), 10)
	fetcher := NewFetcher(conn)
	_, err := fetcher.Fetch()
	if assert.Error(t, err) {
		assert.Equal(t, ErrBadReplyType, err)
	}

	conn = _createRepeatConn([]byte("deep dark fantasy\r\n"), 10)
	fetcher = NewFetcher(conn)
	_, err = fetcher.Fetch()
	if assert.Error(t, err) {
		assert.Equal(t, ErrBadRequest, errors.Cause(err))
	}
}
