package compaction

import (
	"os"
	"testing"

	"github.com/saisushantht/gokv/internal/sstable"
)

// writeSSTTable is a helper that writes an SSTable and returns its path.
func writeSSTable(t *testing.T, entries []struct {
	key, value string
	ts         uint64
	tombstone  bool
}) string {
	t.Helper()
	f, err := os.CreateTemp("", "compact_in_*.sst")
	if err != nil {
		t.Fatal(err)
	}
	path := f.Name()
	f.Close()

	w, err := sstable.NewWriter(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		w.Append([]byte(e.key), []byte(e.value), e.ts, e.tombstone)
	}
	if err := w.Finish(); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestMergeSSTables_Basic(t *testing.T) {
	// Two SSTables with non-overlapping keys
	p1 := writeSSTable(t, []struct {
		key, value string
		ts         uint64
		tombstone  bool
	}{
		{"apple", "red", 1, false},
		{"cherry", "red", 2, false},
	})
	defer os.Remove(p1)

	p2 := writeSSTable(t, []struct {
		key, value string
		ts         uint64
		tombstone  bool
	}{
		{"banana", "yellow", 3, false},
		{"date", "brown", 4, false},
	})
	defer os.Remove(p2)

	out, err := os.CreateTemp("", "compact_out_*.sst")
	if err != nil {
		t.Fatal(err)
	}
	outPath := out.Name()
	out.Close()
	defer os.Remove(outPath)

	if err := MergeSSTables([]string{p1, p2}, outPath, false); err != nil {
		t.Fatal(err)
	}

	r, err := sstable.OpenReader(outPath)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	var keys []string
	r.Iterate(func(e sstable.Entry) {
		keys = append(keys, string(e.Key))
	})

	want := []string{"apple", "banana", "cherry", "date"}
	if len(keys) != len(want) {
		t.Fatalf("got %d keys, want %d: %v", len(keys), len(want), keys)
	}
	for i, k := range want {
		if keys[i] != k {
			t.Errorf("position %d: got %q want %q", i, keys[i], k)
		}
	}
}

func TestMergeSSTables_DeduplicatesKeys(t *testing.T) {
	// Same key in two SSTables — newer timestamp should win
	p1 := writeSSTable(t, []struct {
		key, value string
		ts         uint64
		tombstone  bool
	}{
		{"foo", "old", 1, false},
	})
	defer os.Remove(p1)

	p2 := writeSSTable(t, []struct {
		key, value string
		ts         uint64
		tombstone  bool
	}{
		{"foo", "new", 2, false},
	})
	defer os.Remove(p2)

	out, _ := os.CreateTemp("", "compact_dedup_*.sst")
	outPath := out.Name()
	out.Close()
	defer os.Remove(outPath)

	MergeSSTables([]string{p1, p2}, outPath, false)

	r, _ := sstable.OpenReader(outPath)
	defer r.Close()

	e, ok, err := r.Get([]byte("foo"))
	if err != nil || !ok {
		t.Fatalf("Get(foo): ok=%v err=%v", ok, err)
	}
	if string(e.Value) != "new" {
		t.Errorf("got %q, want %q", e.Value, "new")
	}

	// Verify only one entry exists
	count := 0
	r.Iterate(func(e sstable.Entry) { count++ })
	if count != 1 {
		t.Errorf("expected 1 entry after dedup, got %d", count)
	}
}

func TestMergeSSTables_PurgesTombstones(t *testing.T) {
	p1 := writeSSTable(t, []struct {
		key, value string
		ts         uint64
		tombstone  bool
	}{
		{"alive", "yes", 1, false},
		{"dead", "", 2, true}, // tombstone
	})
	defer os.Remove(p1)

	out, _ := os.CreateTemp("", "compact_tomb_*.sst")
	outPath := out.Name()
	out.Close()
	defer os.Remove(outPath)

	MergeSSTables([]string{p1}, outPath, true) // purgeTombstones=true

	r, _ := sstable.OpenReader(outPath)
	defer r.Close()

	count := 0
	r.Iterate(func(e sstable.Entry) { count++ })
	if count != 1 {
		t.Errorf("expected 1 entry after tombstone purge, got %d", count)
	}

	_, ok, _ := r.Get([]byte("dead"))
	if ok {
		t.Error("tombstoned key should not appear after purge")
	}
}

func TestCompactor_MaybeCompact(t *testing.T) {
	dir, err := os.MkdirTemp("", "compactor_*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	// Write 4 SSTables directly into dir
	paths := make([]string, 4)
	for i := range paths {
		f, _ := os.CreateTemp(dir, "00000*.sst")
		path := f.Name()
		f.Close()

		w, _ := sstable.NewWriter(path)
		w.Append([]byte(string(rune('a'+i))), []byte("val"), uint64(i+1), false)
		w.Finish()
		paths[i] = path
	}

	// Newest first (as DB stores them)
	for i, j := 0, len(paths)-1; i < j; i, j = i+1, j-1 {
		paths[i], paths[j] = paths[j], paths[i]
	}

	c := NewCompactor(dir, 4) // threshold = 4
	newPaths, err := c.MaybeCompact(paths)
	if err != nil {
		t.Fatalf("MaybeCompact: %v", err)
	}

	if len(newPaths) != 1 {
		t.Errorf("expected 1 merged SSTable, got %d", len(newPaths))
	}

	// Verify merged file contains all 4 keys
	r, err := sstable.OpenReader(newPaths[0])
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	count := 0
	r.Iterate(func(e sstable.Entry) { count++ })
	if count != 4 {
		t.Errorf("expected 4 entries in merged SSTable, got %d", count)
	}
}