package hashkit

import (
	"errors"
	"sync"
)

const musk = 0x3fff

// error
var (
	ErrWrongSlaveArgs  = errors.New("arguments a invalid")
	ErrMasterNotExists = errors.New("master didn't exists")
)

// slotsMap is the struct of redis clusters slotsMap
type slotsMap struct {
	// slaves/searchIndex has the same length of masters
	masters     []string
	searchIndex [][]int

	slots []int

	lock sync.Mutex
}

// newRedisClusterRing will create new redis clusters.
func newRedisClusterRing() Ring {
	s := &slotsMap{
		slots: make([]int, musk+1),
		lock:  sync.Mutex{},
	}
	// s.Init(masters, slots...)
	return s
}

func (s *slotsMap) Init(masters []string, args ...[]int) {
	s.lock.Lock()
	s.masters = make([]string, len(masters))
	copy(s.masters, masters)
	for i := range s.masters {
		for _, slot := range args[i] {
			s.slots[slot] = i
		}
		s.searchIndex[i] = args[i]
	}
	s.lock.Unlock()
}

func (s *slotsMap) find(n string) int {
	return findStringSlice(s.masters, n)
}

// AddNode will add node into this ring
// if the ring is ketma hash args contains only one.
func (s *slotsMap) AddNode(node string, args ...int) {
	s.lock.Lock()
	idx := s.find(node)
	if idx == -1 {
		idx = len(s.masters)
		s.masters = append(s.masters, node)
		s.searchIndex = append(s.searchIndex, make([]int, 0))
	}

	for _, slot := range args {
		s.slots[slot] = idx
		s.searchIndex[idx] = append(s.searchIndex[idx], slot)
	}
	s.lock.Unlock()
}

// DelNode will remove node.
func (s *slotsMap) DelNode(node string) {
	s.lock.Lock()
	idx := s.find(node)
	if idx == -1 {
		s.lock.Unlock()
		return
	}

	for i, ival := range s.slots {
		if ival == idx {
			s.slots[i] = -1
		}
	}
	s.searchIndex[idx] = s.searchIndex[idx][:0]
	s.lock.Unlock()
}

// UpdateSlot will update single one slot due to redis move/ask
func (s *slotsMap) UpdateSlot(node string, slot int) {
	s.lock.Lock()
	idx := s.find(node)

	if idx == -1 {
		idx = len(s.masters)
		s.masters = append(s.masters, node)
	}

	s.slots[slot] = idx
}

func (s *slotsMap) Hash(key []byte) int {
	return int(Crc16(key) & musk)
}

func (s *slotsMap) GetNode(key []byte) (string, bool) {
	s.lock.Lock()
	defer s.lock.Unlock()
	slot := Crc16(key) & musk
	idx := s.slots[slot]
	if idx == -1 {
		return "", false
	}

	return s.masters[idx], true
}

func findStringSlice(slice []string, item string) int {
	for i := range slice {
		if slice[i] == item {
			return i
		}
	}
	return -1
}
