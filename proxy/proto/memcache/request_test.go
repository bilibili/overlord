package memcache

import (
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"
)

var _allReqTypes = []RequestType{
	RequestTypeUnknown,
	RequestTypeSet,
	RequestTypeAdd,
	RequestTypeReplace,
	RequestTypeAppend,
	RequestTypePrepend,
	RequestTypeCas,
	RequestTypeGet,
	RequestTypeGets,
	RequestTypeDelete,
	RequestTypeIncr,
	RequestTypeDecr,
	RequestTypeTouch,
	RequestTypeGat,
	RequestTypeGats,
}

func TestRequestTypeString(t *testing.T) {
	reg := regexp.MustCompile(`[a-z]+`)
	for _, rtype := range _allReqTypes {
		assert.True(t, reg.Match(rtype.Bytes()))
		assert.True(t, reg.MatchString(rtype.String()))
	}
}

func TestMCRequestFuncsOk(t *testing.T) {
	req := &MCRequest{
		respType: RequestTypeGet,
		key:      []byte("abc"),
		data:     []byte("\r\n"),
	}
	assert.Equal(t, []byte("get"), req.Cmd())
	assert.Equal(t, "abc", string(req.Key()))
	assert.Equal(t, "type:get key:abc data:\r\n", req.String())

	req.Put()

	assert.Equal(t, RequestTypeUnknown, req.respType)
	assert.Equal(t, []byte{}, req.key)
	assert.Equal(t, []byte{}, req.data)
}
