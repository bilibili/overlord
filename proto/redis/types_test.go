package redis

import (
	"strconv"
	"testing"

	"github.com/felixhao/overlord/proto"
	"github.com/stretchr/testify/assert"
)

func TestRRequestBatchMGETOk(t *testing.T) {
	req := NewCommand("MGET", "mykey", "yourkey")
	reqs, _ := req.Batch()
	assert.Len(t, reqs, 2)
}

func TestRRequestBatchMSETOk(t *testing.T) {
	req := NewCommand("MSET", "mykey", "hello", "yourkey", "1")
	reqs, _ := req.Batch()
	assert.Len(t, reqs, 2)
}

func TestRRequestBatchBasicOk(t *testing.T) {
	req := NewCommand("GET", "mykey")
	reqs, _ := req.Batch()
	assert.Len(t, reqs, 1)
}

func _createMergeReqs(mergeType MergeType, rr *RRequest, resps []*resp) []proto.Request {
	protoResponses := []*RResponse{}

	for _, robj := range resps {
		protoResponses = append(protoResponses, newRResponse(mergeType, robj))
	}

	reqs := make([]proto.Request, len(protoResponses))
	for idx, protoResponse := range protoResponses {
		response := &proto.Response{Type: proto.CacheTypeRedis}
		response.WithProto(protoResponse)
		req := &proto.Request{Type: proto.CacheTypeRedis}
		req.WithProto(rr)
		req.Resp = response
		reqs[idx] = *req
	}

	return reqs
}

func TestRResponseMergeBasicOk(t *testing.T) {
	reqs := _createMergeReqs(MergeTypeBasic, NewCommand("GET", "MYKEY"), []*resp{newRespInt(10)})
	protoResponse := newRResponse(MergeTypeBasic, nil)
	protoResponse.Merge(reqs)
	assert.Equal(t, respInt, protoResponse.respObj.rtype)
	assert.Equal(t, []byte("10"), protoResponse.respObj.data)
}

func TestRResponseMergeJoinOk(t *testing.T) {
	reqs := _createMergeReqs(MergeTypeJoin,
		NewCommand("MGET", "MYKEY", "YOURKEY"),
		[]*resp{newRespString("hello"), newRespString("world")})
	protoResponse := newRResponse(MergeTypeJoin, nil)
	protoResponse.Merge(reqs)

	assert.Equal(t, respArray, protoResponse.respObj.rtype)
	assert.Equal(t, "hello", string(protoResponse.respObj.nth(0).data))
	assert.Equal(t, "world", string(protoResponse.respObj.nth(1).data))
}

func TestRResponseMergeJoinWithNullArrayOk(t *testing.T) {
	reqs := _createMergeReqs(MergeTypeJoin,
		NewCommand("MGET", "MYKEY", "YOURKEY"),
		[]*resp{newRespString("hello"), newRespString("world")})
	protoResponse := newRResponse(MergeTypeJoin, newRespArray(nil))
	assert.True(t, protoResponse.respObj.isNull())
	protoResponse.Merge(reqs)

	assert.Equal(t, respArray, protoResponse.respObj.rtype)
	assert.Equal(t, "hello", string(protoResponse.respObj.nth(0).data))
	assert.Equal(t, "world", string(protoResponse.respObj.nth(1).data))
}

func TestRResponseMergeCountOk(t *testing.T) {
	reqs := _createMergeReqs(MergeTypeCount,
		NewCommand("EXISTS", "MYKEY", "YOURKEY"),
		[]*resp{newRespInt(1), newRespInt(0)})
	protoResponse := newRResponse(MergeTypeCount, nil)
	protoResponse.Merge(reqs)
	assert.Equal(t, respInt, protoResponse.respObj.rtype)
	ival, err := strconv.Atoi(string(protoResponse.respObj.data))
	assert.NoError(t, err)
	assert.Equal(t, 1, ival)
}

func TestRResponseMergeOkOk(t *testing.T) {
	reqs := _createMergeReqs(MergeTypeOk,
		NewCommand("MSET", "MYKEY", "VAL1", "YOURKEY", "VAL2"),
		[]*resp{newRespString("OK"), newRespString("OK")})
	protoResponse := newRResponse(MergeTypeOk, nil)
	protoResponse.Merge(reqs)
	assert.Equal(t, respString, protoResponse.respObj.rtype)
	assert.Equal(t, "OK", string(protoResponse.respObj.data))
}
