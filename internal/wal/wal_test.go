package wal

import (
	"os"
	"testing"
)

func TestWAL_AppendAndReplay(t *testing.T) {
	// Create a temp file for the WAL
	f, err := os.CreateTemp("", "wal_test_*.wal")
	if err != nil {
		t.Fatal(err)
	}
	path := f.Name()
	f.Close()
	defer os.Remove(path)

	// Open WAL and write 3 entries
	w, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	entries := []struct {
		key, value []byte
		ts         uint64
		tombstone  bool
	}{
		{[]byte("foo"), []byte("bar"), 1, false},
		{[]byte("hello"), []byte("world"), 2, false},
		{[]byte("delete-me"), []byte(""), 3, true},
	}

	for _, e := range entries {
		if err := w.Append(e.key, e.value, e.ts, e.tombstone); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}

	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reopen and replay
	w2, err := Open(path)
	if err != nil {
		t.Fatalf("Open for replay: %v", err)
	}
	defer w2.Close()

	i := 0
	w2.Replay(func(key, value []byte, ts uint64, tombstone bool) {
		if i >= len(entries) {
			t.Fatalf("too many entries replayed")
		}
		e := entries[i]
		if string(key) != string(e.key) {
			t.Errorf("entry %d: key = %q, want %q", i, key, e.key)
		}
		if string(value) != string(e.value) {
			t.Errorf("entry %d: value = %q, want %q", i, value, e.value)
		}
		if ts != e.ts {
			t.Errorf("entry %d: ts = %d, want %d", i, ts, e.ts)
		}
		if tombstone != e.tombstone {
			t.Errorf("entry %d: tombstone = %v, want %v", i, tombstone, e.tombstone)
		}
		i++
	})

	if i != len(entries) {
		t.Errorf("replayed %d entries, want %d", i, len(entries))
	}
}

func TestWAL_PartialEntry(t *testing.T) {
	f, err := os.CreateTemp("", "wal_partial_*.wal")
	if err != nil {
		t.Fatal(err)
	}
	path := f.Name()
	f.Close()
	defer os.Remove(path)

	// Write one good entry
	w, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	w.Append([]byte("good"), []byte("entry"), 1, false)
	w.Close()

	// Corrupt the file by appending partial data
	file, _ := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0644)
	file.Write([]byte{0x05, 0x00, 0x00, 0x00}) // key length = 5, but no key follows
	file.Close()

	// Replay should return the one good entry and stop gracefully
	w2, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()

	count := 0
	w2.Replay(func(key, value []byte, ts uint64, tombstone bool) {
		count++
	})

	if count != 1 {
		t.Errorf("expected 1 entry, got %d", count)
	}
}