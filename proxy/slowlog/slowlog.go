package slowlog

import (
	"overlord/proxy/proto"
	"sync/atomic"
	"sync"
	"fmt"
)

const slowlogMaxCount = 1024

func newStore(name string) *Store {
	return &Store{
		name:   name,
		cursor: 0,
		msgs:   make([]atomic.Value, slowlogMaxCount),
	}
}

// Store is the collector of slowlog
type Store struct {
	name   string
	cursor int32
	msgs   []atomic.Value
}

// Record impl the Handler
func (s *Store) Record(msg *proto.SlowlogEntry) {
	if msg == nil {
		return
	}

	for {
		if atomic.CompareAndSwapInt32(&s.cursor, s.cursor, s.cursor+1) {
			idx := s.cursor % slowlogMaxCount
			s.msgs[idx].Store(msg)
			fmt.Println(msg.String())
			break
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
		sentry := m.(*proto.SlowlogEntry)
		entries = append(entries, sentry)
	}

	ses := &proto.SlowlogEntries{
		Cluster: s.name,
		Entries: entries,
	}
	return ses
}

var (
	storeMap  = map[string]*Store{}
	storeLock sync.RWMutex
)

// Handler is the handler which contains the store instance with async call
type Handler interface {
	Record(msg *proto.SlowlogEntry)
	Reply() *proto.SlowlogEntries
}

// Get create the message Handler or get the exists one
func Get(name string) Handler {
	storeLock.RLock()
	if s, ok := storeMap[name]; ok {
		handler := &storeHandler{store: s}
		storeLock.RUnlock()
		return handler
	}
	storeLock.RUnlock()

	storeLock.Lock()
	defer storeLock.Unlock()
	s := newStore(name)
	storeMap[name] = s
	return &storeHandler{store: s}
}

type storeHandler struct {
	store *Store
}

func (sr *storeHandler) Record(msg *proto.SlowlogEntry) {
	sr.store.Record(msg)
}

func (sr *storeHandler) Reply() *proto.SlowlogEntries {
	return sr.store.Reply()
}
