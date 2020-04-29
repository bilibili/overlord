package chunk

import (
	"fmt"
	"testing"

	ms "github.com/mesos/mesos-go/api/v1/lib"

	"github.com/stretchr/testify/assert"
)

func _createOffers(count int, memory float64, cpu float64, portBegin, portEnd uint64) []ms.Offer {
	offers := []ms.Offer{}
	for i := 0; i < count; i++ {
		offer := ms.Offer{
			ID:       ms.OfferID{Value: fmt.Sprintf("offer-%d", i)},
			Hostname: fmt.Sprintf("host-%d", i),
			Resources: []ms.Resource{
				ms.Resource{
					Name:   ResNameCPUs,
					Scalar: &ms.Value_Scalar{Value: cpu},
				},
				ms.Resource{
					Name:   ResNameMem,
					Scalar: &ms.Value_Scalar{Value: memory},
				},
				ms.Resource{
					Name: ResNamePorts,
					Ranges: &ms.Value_Ranges{
						Range: []ms.Value_Range{{Begin: portBegin, End: portEnd}},
					},
				},
			},
		}
		offers = append(offers, offer)
	}

	return offers
}

func TestChunksCalcByMemoryOk(t *testing.T) {
	offers := _createOffers(3, 128*1024, 32, 7000, 8000)
	chunks, err := Chunks(6, 100.0, 1.0, offers...)
	assert.NoError(t, err)
	assert.Len(t, chunks, 3)
}

func TestChunksAppend(t *testing.T) {
	offers := _createOffers(6, 128*1024, 32, 7000, 8000)
	chunks, err := Chunks(6, 100.0, 1.0, offers...)
	assert.NoError(t, err)
	assert.Len(t, chunks, 3)
	//chunks append by old chunks
	newChunks, err := ChunksAppend(chunks, 2, 100.0, 1, offers...)
	assert.NoError(t, err, "append by same offer host")
	assert.Len(t, newChunks, 1)
	offers = _createOffers(7, 128*1024, 32, 7000, 8000)
	newChunks, err = ChunksAppend(chunks, 2, 100.0, 1, offers...)
	assert.NoError(t, err, "append by more offer host")
	assert.Len(t, newChunks, 1)
	offers = _createOffers(4, 128*1024, 32, 7000, 8000)
	newChunks, err = ChunksAppend(chunks, 2, 100.0, 1, offers...)
	assert.NoError(t, err, "append by less offer host")
	assert.Len(t, newChunks, 1)
	offers = _createOffers(10, 128*1024, 32, 7000, 8000)
	newChunks, err = ChunksAppend(chunks, 2, 100.0, 1, offers[6:]...)
	assert.NoError(t, err, "append by all new offer host")
	assert.Len(t, newChunks, 1)
}

func TestChunksRecover(t *testing.T) {
	offers := _createOffers(6, 128*1024, 32, 7000, 8000)
	chunks, err := Chunks(6, 100.0, 1.0, offers...)
	assert.NoError(t, err)
	assert.Len(t, chunks, 3)
	t.Logf("before recover %v", chunks)
	disabledHost := ValidateIPAddress(offers[5].Hostname)
	newChunk, err := ChunksRecover(chunks, disabledHost, 100, 1, offers[:5]...)
	assert.NoError(t, err)
	assert.Len(t, newChunk, 3)
	t.Logf("after recover %v", newChunk)

	chunks, err = Chunks(10, 100, 1, offers...)
	assert.NoError(t, err)
	assert.Len(t, chunks, 5)
	t.Logf("before recover %v", chunks)
	disabledHost = ValidateIPAddress(offers[5].Hostname)
	newChunk, err = ChunksRecover(chunks, disabledHost, 100, 1, offers[:5]...)
	assert.NoError(t, err)
	assert.Len(t, newChunk, 5)
	t.Logf("after recover %v", newChunk)

	chunks, err = Chunks(12, 100, 1, offers...)
	assert.NoError(t, err)
	assert.Len(t, chunks, 6)
	t.Logf("before recover %v", chunks)
	disabledHost = ValidateIPAddress(offers[5].Hostname)
	newChunk, err = ChunksRecover(chunks, disabledHost, 100, 1, offers[:5]...)
	assert.NoError(t, err)
	assert.Len(t, newChunk, 6)
	t.Logf("after recover %v", newChunk)
}

func TestChunksCalcByLowMemory(t *testing.T) {
	offers := _createOffers(3, 1, 32, 7000, 8000)
	chunks, err := Chunks(6, 100.0, 1.0, offers...)
	assert.Error(t, err)
	assert.Equal(t, ErrNotEnoughResource, err)
	assert.Len(t, chunks, 0)
}

func TestChunksCalcErrorWithNot3Not4(t *testing.T) {
	offers := _createOffers(3, 1, 32, 7000, 8000)
	chunks, err := Chunks(4, 10, 1, offers...)
	assert.Error(t, err)
	assert.Equal(t, ErrNot3Not4, err)
	assert.Len(t, chunks, 0)
}

func TestChunksCalcByLowCPU(t *testing.T) {
	offers := _createOffers(3, 1024*128, 1, 7000, 8000)
	chunks, err := Chunks(6, 100.0, 1.0, offers...)
	assert.Error(t, err)
	assert.Equal(t, err, ErrNotEnoughResource)
	assert.Len(t, chunks, 0)
}

func TestNodeIntoConfLineOk(t *testing.T) {
	node := &Node{
		Name:    "127.0.0.1",
		Port:    7000,
		Role:    RoleMaster,
		RunID:   "001",
		SlaveOf: "-",
		Slots: []Slot{
			{1, 1},
			{12, 20},
			{700, 800},
		},
	}

	line := node.IntoConfLine(true)
	assert.Equal(t, "0000000000000000000000000000000000000001 127.0.0.1:7000@17000 myself,master - 0 0 0 connected 1 12-20 700-800\n", line)

	line = node.IntoConfLine(false)
	assert.Equal(t, "0000000000000000000000000000000000000001 127.0.0.1:7000@17000 master - 0 0 0 connected 1 12-20 700-800\n", line)
}
