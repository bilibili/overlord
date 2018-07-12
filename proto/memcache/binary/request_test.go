package binary

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

var _allReqTypes = []RequestType{
	RequestTypeGet,
	RequestTypeSet,
	RequestTypeAdd,
	RequestTypeReplace,
	RequestTypeDelete,
	RequestTypeIncr,
	RequestTypeDecr,
	RequestTypeGetQ,
	RequestTypeNoop,
	RequestTypeGetK,
	RequestTypeGetKQ,
	RequestTypeAppend,
	RequestTypePrepend,
	RequestTypeTouch,
	RequestTypeGat,
	RequestTypeUnknown,
}

func TestRequestTypeBytes(t *testing.T) {
	for _, rtype := range _allReqTypes {
		assert.Equal(t, []byte{byte(rtype)}, rtype.Bytes())
	}
	assert.Equal(t, "get", RequestTypeGet.String())
}

func TestMCRequestFuncsOk(t *testing.T) {
	req := &MCRequest{
		rTp:  RequestTypeGet,
		key:  []byte("abc"),
		data: []byte("\r\n"),
	}
	assert.Equal(t, []byte{byte(RequestTypeGet)}, req.Cmd())
	assert.Equal(t, "abc", string(req.Key()))
	assert.Equal(t, "type:get key:abc data:\r\n", req.String())
}
