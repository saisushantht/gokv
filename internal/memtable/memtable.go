package memtable

import "sync"

// Memtable is a concurrent in-memory sorted structure.
// Writes go here first (after WAL), then flush to SSTable when full.
type Memtable struct {
	mu   sync.RWMutex
	sl   *skipList
}

func New() *Memtable {
	return &Memtable{sl: newSkipList()}
}

// Put inserts or updates a key.
func (m *Memtable) Put(key, value []byte, ts uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sl.put(key, value, ts, false)
}

// Delete marks a key as deleted via a tombstone.
func (m *Memtable) Delete(key []byte, ts uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sl.put(key, nil, ts, true)
}

// Get returns the value for a key.
// Returns (nil, false) if not found.
// Returns (nil, true) if found but tombstoned (deleted).
func (m *Memtable) Get(key []byte) ([]byte, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	e, ok := m.sl.get(key)
	if !ok {
		return nil, false
	}
	if e.tombstone {
		return nil, true // found but deleted
	}
	return e.value, true
}

// Size returns approximate memory usage in bytes.
func (m *Memtable) Size() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.sl.size
}

// Iterate calls fn for every entry in sorted order.
// Used during flush to write SSTable.
func (m *Memtable) Iterate(fn func(key, value []byte, ts uint64, tombstone bool)) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	m.sl.iterate(func(e entry) {
		fn(e.key, e.value, e.ts, e.tombstone)
	})
}