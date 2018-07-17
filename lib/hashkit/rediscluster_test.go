package hashkit

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func _createSlotsMap(n int) *slotsMap {
	r := newRedisClusterRing()
	addrs := make([]string, n)
	for i := 0; i < n; i++ {
		addrs[i] = fmt.Sprintf("127.0.0.1:70%02d", i)
	}
	rg := (musk + 1) / n
	slots := make([][]int, n)
	for i := 0; i < n; i++ {
		iSlots := make([]int, 0)
		start, end := i*rg, (i+1)*rg
		for j := start; j < end; j++ {
			iSlots = append(iSlots, j)
		}
		slots[i] = iSlots
	}
	for i := n * rg; i < musk+1; i++ {
		slots[n-1] = append(slots[n-1], i)
	}
	// fmt.Println(addrs, slots)
	r.Init(addrs, slots...)
	return r.(*slotsMap)
}

func TestSlotsMapAddNodeOk(t *testing.T) {
	sm := _createSlotsMap(10)
	sm.AddNode("127.0.0.2:7000", []int{0, 1, 2}...)
	// the 11th
	assert.Equal(t, "127.0.0.2:7000", sm.masters[10])
	assert.Equal(t, 10, sm.slots[0])
	assert.Equal(t, 10, sm.slots[1])
	assert.Equal(t, 10, sm.slots[2])
}

func TestSlotsMapDelNodeOk(t *testing.T) {
	sm := _createSlotsMap(10)
	sm.DelNode("127.0.0.1:7000")
	// the 11th
	assert.Equal(t, "127.0.0.1:7000", sm.masters[0])
	assert.Equal(t, -1, sm.slots[0])
	assert.Len(t, sm.searchIndex[0], 0)
}

func TestSlotsMapUpdateSlot(t *testing.T) {
	sm := _createSlotsMap(10)
	sm.UpdateSlot("127.0.0.2:7000", 1)

	assert.Equal(t, "127.0.0.2:7000", sm.masters[10])
	assert.Equal(t, 10, sm.slots[1])
}

func TestSlotsMapHashOk(t *testing.T) {
	sm := _createSlotsMap(10)
	crc := sm.Hash([]byte("abcdefg"))
	// assert.True(t, ok)
	assert.Equal(t, 13912, crc)
}

func TestGetNodeOk(t *testing.T) {
	sm := _createSlotsMap(10)
	// t.Log(sm.masters)
	// t.Log(sm.slots)
	node, ok := sm.GetNode([]byte("abcdefg"))
	assert.True(t, ok)
	assert.Equal(t, "127.0.0.1:7008", node)
}
