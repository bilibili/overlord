package slowlog

import (
	"overlord/proxy/proto"
	"sync/atomic"
)

const slowlogMaxCount = 1024

func newStore(name string) *Store {
	return &Store{
		name:   name,
		count:  0,
		cursor: 0,
		msgs:   make([]atomic.Value, slowlogMaxCount),
	}
}

// Store is the collector of slowlog
type Store struct {
	name   string
	count  int32
	cursor int32
	msgs   []atomic.Value
}

func (s *Store) push(msg *proto.Message) {
	if msg == nil {
		return
	}

	for {
		if atomic.CompareAndSwapInt32(&s.cursor, s.cursor, s.cursor+1) {
			idx := s.cursor % s.count
			s.msgs[idx].Store(msg)
		}
	}
}

// Reply impl the Replyer
func (s *Store) Reply() *proto.SlowlogEntries {
	entries := make([]*proto.SlowlogEntry, 0)
	for _, msg := range s.msgs {
		m := msg.Load()
		if m == nil {
			break
		}
		protoMsg := m.(*proto.Message)
		entries = append(entries, protoMsg.Slowlog())
	}

	ses := &proto.SlowlogEntries{
		Cluster: s.name,
		Entries: entries,
	}
	return ses
}

var storeMap = map[string]*Store{}

// Replyer will reply the given store
type Replyer interface {
	Reply() *proto.SlowlogEntries
}

// NewReplyer create or get current Replyer
func NewReplyer(name string) Replyer {
	if s, ok := storeMap[name]; ok {
		return &storeReplyer{store: s}
	}
	panic("replyer must be create after recorder")
}

type storeReplyer struct {
	store *Store
}

func (sr *storeReplyer) Reply() *proto.SlowlogEntries {
	return sr.store.Reply()
}

// Recorder is the handler which contains the store instance with async call
type Recorder interface {
	Record(msg *proto.Message)
}

// NewRecorder create the message Recorder or get the exists one
func NewRecorder(name string) Recorder {
	if s, ok := storeMap[name]; ok {
		return &storeRecorder{store: s}
	}

	s := newStore(name)
	storeMap[name] = s
	return NewRecorder(name)
}

type storeRecorder struct {
	store *Store
}

func (sr *storeRecorder) Record(msg *proto.Message) {
	sr.store.push(msg)
}
