package trie

// Trie is the struct which contains a array based trie implemented.
type Trie struct {
	isTail bool
	subs   map[byte]*Trie
}

// New create new zero Trie.
func New() *Trie {
	return &Trie{
		isTail: false,
		subs: nil,
	}
}

// Init the trie with given keys.
func (t *Trie) Init(keys []string) {
	for _, key := range keys {
		t.Add([]byte(key))
	}
}

// Add add new key for Trie
func (t *Trie) Add(key []byte) {
	if len(key) == 0 {
		t.isTail = true
		return
	}

	if len(t.subs) == 0 {
		t.subs = make(map[byte]*Trie)
	}

	c := key[0]

	if _, ok := t.subs[c]; !ok {
		t.subs[c] = &Trie{
			isTail: false,
			subs:   nil,
		}
	}
	t.subs[c].Add(key[1:])
}


// Contains returns true if key is contained by the trie.
func (t *Trie) Contains(key []byte) bool {
	if len(key) == 0 {
		return t.isTail
	}

	c := key[0]
	if len(t.subs) == 0 {
		return false
	}
	if sub, ok := t.subs[c]; ok {
		return sub.Contains(key[1:])
	}
	return false
}
