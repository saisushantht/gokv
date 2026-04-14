package memtable

import (
	"math/rand"
)

const maxLevel = 16
const probability = 0.5

type entry struct {
	key       []byte
	value     []byte
	ts        uint64
	tombstone bool
}

type node struct {
	entry
	next []*node
}

type skipList struct {
	head  *node
	level int
	size  int64 // tracked in bytes
}

func newSkipList() *skipList {
	head := &node{next: make([]*node, maxLevel)}
	return &skipList{head: head, level: 1}
}

// randomLevel picks how many levels a new node participates in.
func randomLevel() int {
	level := 1
	for level < maxLevel && rand.Float64() < probability {
		level++
	}
	return level
}

// put inserts or updates a key. Latest timestamp wins.
func (s *skipList) put(key, value []byte, ts uint64, tombstone bool) {
	// update[i] holds the rightmost node at level i that is < key
	update := make([]*node, maxLevel)
	curr := s.head

	for i := s.level - 1; i >= 0; i-- {
		for curr.next[i] != nil && string(curr.next[i].key) < string(key) {
			curr = curr.next[i]
		}
		update[i] = curr
	}

	// Check if key already exists at level 0
	existing := update[0].next[0]
	if existing != nil && string(existing.key) == string(key) {
		// Update in place — subtract old size, add new
		s.size -= int64(len(existing.key) + len(existing.value))
		existing.value = value
		existing.ts = ts
		existing.tombstone = tombstone
		s.size += int64(len(key) + len(value))
		return
	}

	// New node
	level := randomLevel()
	if level > s.level {
		for i := s.level; i < level; i++ {
			update[i] = s.head
		}
		s.level = level
	}

	n := &node{
		entry: entry{key: key, value: value, ts: ts, tombstone: tombstone},
		next:  make([]*node, level),
	}

	for i := 0; i < level; i++ {
		n.next[i] = update[i].next[i]
		update[i].next[i] = n
	}

	s.size += int64(len(key) + len(value))
}

// get returns the entry for a key, or false if not found.
func (s *skipList) get(key []byte) (entry, bool) {
	curr := s.head
	for i := s.level - 1; i >= 0; i-- {
		for curr.next[i] != nil && string(curr.next[i].key) < string(key) {
			curr = curr.next[i]
		}
	}
	curr = curr.next[0]
	if curr != nil && string(curr.key) == string(key) {
		return curr.entry, true
	}
	return entry{}, false
}

// iterate calls fn for every entry in sorted key order.
func (s *skipList) iterate(fn func(e entry)) {
	curr := s.head.next[0]
	for curr != nil {
		fn(curr.entry)
		curr = curr.next[0]
	}
}