package memtable

import (
	"fmt"
	"sync"
	"testing"
)

func TestMemtable_PutAndGet(t *testing.T) {
	m := New()

	m.Put([]byte("foo"), []byte("bar"), 1)
	val, ok := m.Get([]byte("foo"))
	if !ok {
		t.Fatal("expected to find key foo")
	}
	if string(val) != "bar" {
		t.Errorf("got %q, want %q", val, "bar")
	}
}

func TestMemtable_Overwrite(t *testing.T) {
	m := New()
	m.Put([]byte("foo"), []byte("bar"), 1)
	m.Put([]byte("foo"), []byte("baz"), 2)

	val, ok := m.Get([]byte("foo"))
	if !ok {
		t.Fatal("expected to find key foo")
	}
	if string(val) != "baz" {
		t.Errorf("got %q, want %q", val, "baz")
	}
}

func TestMemtable_Delete(t *testing.T) {
	m := New()
	m.Put([]byte("foo"), []byte("bar"), 1)
	m.Delete([]byte("foo"), 2)

	val, ok := m.Get([]byte("foo"))
	if !ok {
		t.Fatal("expected tombstone entry to be found")
	}
	if val != nil {
		t.Errorf("expected nil value for tombstone, got %q", val)
	}
}

func TestMemtable_MissingKey(t *testing.T) {
	m := New()
	_, ok := m.Get([]byte("missing"))
	if ok {
		t.Error("expected false for missing key")
	}
}

func TestMemtable_SortedIteration(t *testing.T) {
	m := New()
	keys := []string{"banana", "apple", "cherry", "date"}
	for i, k := range keys {
		m.Put([]byte(k), []byte(fmt.Sprintf("v%d", i)), uint64(i))
	}

	var got []string
	m.Iterate(func(key, value []byte, ts uint64, tombstone bool) {
		got = append(got, string(key))
	})

	want := []string{"apple", "banana", "cherry", "date"}
	for i, k := range want {
		if got[i] != k {
			t.Errorf("position %d: got %q, want %q", i, got[i], k)
		}
	}
}

func TestMemtable_ConcurrentWrites(t *testing.T) {
	m := New()
	var wg sync.WaitGroup

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := []byte(fmt.Sprintf("key-%d", i))
			m.Put(key, []byte("value"), uint64(i))
		}(i)
	}
	wg.Wait()

	// Verify all 100 keys are present
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		_, ok := m.Get(key)
		if !ok {
			t.Errorf("missing key-%d", i)
		}
	}
}