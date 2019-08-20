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
	assert.Equal(t, getString, RequestTypeGet.String())
	assert.Equal(t, setString, RequestTypeSet.String())
	assert.Equal(t, addString, RequestTypeAdd.String())
	assert.Equal(t, replaceString, RequestTypeReplace.String())
	assert.Equal(t, deleteString, RequestTypeDelete.String())
	assert.Equal(t, incrString, RequestTypeIncr.String())
	assert.Equal(t, decrString, RequestTypeDecr.String())
	assert.Equal(t, getQString, RequestTypeGetQ.String())
	assert.Equal(t, noopString, RequestTypeNoop.String())
	assert.Equal(t, getKString, RequestTypeGetK.String())
	assert.Equal(t, getKQString, RequestTypeGetKQ.String())
	assert.Equal(t, appendString, RequestTypeAppend.String())
	assert.Equal(t, prependString, RequestTypePrepend.String())
	assert.Equal(t, touchString, RequestTypeTouch.String())
	assert.Equal(t, gatString, RequestTypeGat.String())
	assert.Equal(t, unknownString, RequestTypeUnknown.String())
}

func TestMCRequestFuncsOk(t *testing.T) {
	req := newReq()
	req.respType = RequestTypeGet
	req.key = []byte("abc")
	req.data = []byte("\r\n")
	assert.Equal(t, []byte{byte(RequestTypeGet)}, req.Cmd())
	assert.Equal(t, "abc", string(req.Key()))
	assert.Equal(t, "type:get key:abc data:\r\n", req.String())

	req.Put()

	assert.Equal(t, RequestTypeUnknown, req.respType)
	assert.Len(t, req.keyLen, 2)
	assert.Len(t, req.extraLen, 1)
	assert.Len(t, req.status, 2)
	assert.Len(t, req.bodyLen, 4)
	assert.Len(t, req.opaque, 4)
	assert.Len(t, req.cas, 8)
	assert.Len(t, req.key, 0)
	assert.Len(t, req.data, 0)
}
