package redis

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseSlotFieldEmptyLineOk(t *testing.T) {
	slots, ok := parseSlotField("")
	assert.Nil(t, slots)
	assert.False(t, ok)

	slots, ok = parseSlotField("-")
	assert.Nil(t, slots)
	assert.False(t, ok)
}

func TestParseSlotSingleValueOk(t *testing.T) {
	slots, ok := parseSlotField("1024")
	assert.True(t, ok)
	assert.Len(t, slots, 1)
	assert.Equal(t, 1024, slots[0])
}

func TestParseSlotSingleValueBadNumber(t *testing.T) {
	slots, ok := parseSlotField("@boynextdoor")
	assert.False(t, ok)
	assert.Nil(t, slots)
}

func TestParseSlotRangeSecondBadNumber(t *testing.T) {
	slots, ok := parseSlotField("1024-@boynextdoor")
	assert.False(t, ok)
	assert.Nil(t, slots)
}

func TestParseSlotRangeBadRange(t *testing.T) {
	slots, ok := parseSlotField("1024-12")
	assert.False(t, ok)
	assert.Nil(t, slots)
}

func TestParseSlotRangeButOneValue(t *testing.T) {
	slots, ok := parseSlotField("1024-1024")
	assert.True(t, ok)
	assert.Len(t, slots, 1)
	assert.Equal(t, 1024, slots[0])
}

func TestParseSlotRangeOk(t *testing.T) {
	slots, ok := parseSlotField("12-1222")
	assert.True(t, ok)
	assert.Len(t, slots, 1222-12+1)
	assert.Equal(t, 12, slots[0])
	assert.Equal(t, 1222, slots[len(slots)-1])
}

func TestNodeSetOk(t *testing.T) {
	n := &node{}

	cID := "3f76d4dca41307bea25e8f69a3545594479dc7a9"
	n.setID(cID)
	assert.Equal(t, cID, n.ID)

	addr := "127.0.0.1:1024"
	n.setAddr(addr)
	assert.Equal(t, addr, n.Addr())

	addrWithGossip := addr + "@11024"
	n.setAddr(addrWithGossip)
	assert.Equal(t, addr, n.Addr())

	flagLists := []string{
		"mark,myself,master",
		"mark,slave",
		"mark,myself,slave",
		"mark,faild",
	}
	for _, val := range flagLists {
		t.Run("TestNodeSetWithFlags"+val, func(t *testing.T) {
			n.setFlags(val)
			assert.Contains(t, n.Flags(), "mark")
		})
	}

	masterAddr := "127.0.0.1:7788"
	n.setSlaveOf(masterAddr)
	assert.Equal(t, masterAddr, n.SlaveOf())

	// get or null
	n.setPingSent("1024")
	assert.Equal(t, 1024, n.pingSent)

	n.setPingSent("-")
	assert.Equal(t, 0, n.pingSent)

	n.setPongRecv("1024")
	assert.Equal(t, 1024, n.pongRecv)

	n.setPongRecv("-")
	assert.Equal(t, 0, n.pongRecv)

	n.setConfigEpoch("1024")
	assert.Equal(t, 1024, n.configEpoch)

	n.setConfigEpoch("-")
	assert.Equal(t, 0, n.configEpoch)

	link := "zelda"
	n.setLinkState(link)
	assert.Equal(t, link, n.linkState)

	slots := []struct {
		name string
		s    []string
		i    []int
	}{
		{"RangeOk", []string{"1", "1024"}, []int{1, 1024}},
		{"RangeFail", []string{"1", "@", "1024"}, []int{1, 1024}},
	}

	for _, slot := range slots {
		t.Run("TestNodeSetSlots"+slot.name, func(t *testing.T) {
			n.setSlots(slot.s...)
			assert.Equal(t, slot.i, n.Slots)
		})
	}
}
