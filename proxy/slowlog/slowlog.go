package slowlog

import (
	"sync"
	"sync/atomic"

	"overlord/pkg/log"
	"overlord/proxy/proto"
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
	cursor uint32
	msgs   []atomic.Value
}

// Record impl the Handler
func (s *Store) Record(msg *proto.SlowlogEntry) {
	if msg == nil {
		return
	}

	for {
		if atomic.CompareAndSwapUint32(&s.cursor, s.cursor, s.cursor+1) {
			idx := (s.cursor - 1) % slowlogMaxCount
			s.msgs[idx].Store(msg)
			if fh != nil {
				fh.save(s.name, msg)
			}
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
		storeLock.RUnlock()
		return s
	}
	storeLock.RUnlock()

	storeLock.Lock()
	defer storeLock.Unlock()
	s := newStore(name)
	storeMap[name] = s
	return s
}

// Init slowlog with file and http
func Init(fileName string, maxBytes int, backupCount int) error {
	registerSlowlogHTTP()
	if fileName == "" {
		return nil
	}
	log.Infof("setup slowlog for file [%s]", fileName)
	return initFileHandler(fileName, maxBytes, backupCount)
}
