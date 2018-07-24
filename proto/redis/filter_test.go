package redis

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetReqTypeOk(t *testing.T) {
	assert.Equal(t, reqTypeCtl, getReqType([]byte("4\r\nPING")))
	assert.Equal(t, reqTypeRead, getReqType([]byte("3\r\nGET")))
	assert.Equal(t, reqTypeWrite, getReqType([]byte("3\r\nSET")))
	assert.Equal(t, reqTypeNotSupport, getReqType([]byte("4\r\nEVAL")))
	assert.Equal(t, reqTypeNotSupport, getReqType([]byte("baka")))
}
