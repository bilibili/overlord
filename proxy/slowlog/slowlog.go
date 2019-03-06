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

// Record impl the Handler
func (s *Store) Record(msg *proto.Message) {
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

// Handler is the handler which contains the store instance with async call
type Handler interface {
	Record(msg *proto.Message)
	Reply() *proto.SlowlogEntries
}

// New create the message Handler or get the exists one
func New(name string) Handler {
	if s, ok := storeMap[name]; ok {
		return &storeHandler{store: s}
	}

	s := newStore(name)
	storeMap[name] = s
	return New(name)
}

type storeHandler struct {
	store *Store
}

func (sr *storeHandler) Record(msg *proto.Message) {
	sr.store.Record(msg)
}

func (sr *storeHandler) Reply() *proto.SlowlogEntries {
	return sr.store.Reply()
}
