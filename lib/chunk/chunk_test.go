package chunk

import (
	"testing"

	ms "github.com/mesos/mesos-go/api/v1/lib"

	"fmt"

	"github.com/stretchr/testify/assert"
)

/*

type Offer struct {
	ID          OfferID     `protobuf:"bytes,1,req,name=id" json:"id"`
	FrameworkID FrameworkID `protobuf:"bytes,2,req,name=framework_id,json=frameworkId" json:"framework_id"`
	AgentID     AgentID     `protobuf:"bytes,3,req,name=agent_id,json=agentId" json:"agent_id"`
	Hostname    string      `protobuf:"bytes,4,req,name=hostname" json:"hostname"`
	// URL for reaching the agent running on the host.
	URL *URL `protobuf:"bytes,8,opt,name=url" json:"url,omitempty"`
	// The domain of the agent.
	Domain     *DomainInfo `protobuf:"bytes,11,opt,name=domain" json:"domain,omitempty"`
	Resources  []Resource  `protobuf:"bytes,5,rep,name=resources" json:"resources"`
	Attributes []Attribute `protobuf:"bytes,7,rep,name=attributes" json:"attributes"`
	// Executors of the same framework running on this agent.
	ExecutorIDs []ExecutorID `protobuf:"bytes,6,rep,name=executor_ids,json=executorIds" json:"executor_ids"`
	// Signifies that the resources in this Offer may be unavailable during
	// the given interval.  Any tasks launched using these resources may be
	// killed when the interval arrives.  For example, these resources may be
	// part of a planned maintenance schedule.
	//
	// This field only provides information about a planned unavailability.
	// The unavailability interval may not necessarily start at exactly this
	// interval, nor last for exactly the duration of this interval.
	// The unavailability may also be forever!  See comments in
	// `Unavailability` for more details.
	Unavailability *Unavailability `protobuf:"bytes,9,opt,name=unavailability" json:"unavailability,omitempty"`
	// An offer represents resources allocated to *one* of the
	// roles managed by the scheduler. (Therefore, each
	// `Offer.resources[i].allocation_info` will match the
	// top level `Offer.allocation_info`).
	AllocationInfo *Resource_AllocationInfo `protobuf:"bytes,10,opt,name=allocation_info,json=allocationInfo" json:"allocation_info,omitempty"`
}
*/

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
