package sstable

import (
	"os"
	"testing"
)

func TestSSTable_WriteAndRead(t *testing.T) {
	f, err := os.CreateTemp("", "sst_test_*.sst")
	if err != nil {
		t.Fatal(err)
	}
	path := f.Name()
	f.Close()
	defer os.Remove(path)

	w, err := NewWriter(path)
	if err != nil {
		t.Fatal(err)
	}

	entries := []struct {
		key, value string
		ts         uint64
		tombstone  bool
	}{
		{"apple", "red", 1, false},
		{"banana", "yellow", 2, false},
		{"cherry", "", 3, true},
		{"date", "brown", 4, false},
	}

	for _, e := range entries {
		if err := w.Append([]byte(e.key), []byte(e.value), e.ts, e.tombstone); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Finish(); err != nil {
		t.Fatal(err)
	}

	r, err := OpenReader(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	for _, e := range entries {
		got, ok, err := r.Get([]byte(e.key))
		if err != nil {
			t.Fatalf("Get(%q): %v", e.key, err)
		}
		if !ok {
			t.Fatalf("Get(%q): not found", e.key)
		}
		if string(got.Key) != e.key {
			t.Errorf("key: got %q want %q", got.Key, e.key)
		}
		if string(got.Value) != e.value {
			t.Errorf("value: got %q want %q", got.Value, e.value)
		}
		if got.Timestamp != e.ts {
			t.Errorf("ts: got %d want %d", got.Timestamp, e.ts)
		}
		if got.Tombstone != e.tombstone {
			t.Errorf("tombstone: got %v want %v", got.Tombstone, e.tombstone)
		}
	}
}

func TestSSTable_GetMissing(t *testing.T) {
	f, err := os.CreateTemp("", "sst_miss_*.sst")
	if err != nil {
		t.Fatal(err)
	}
	path := f.Name()
	f.Close()
	defer os.Remove(path)

	w, _ := NewWriter(path)
	w.Append([]byte("apple"), []byte("red"), 1, false)
	w.Finish()

	r, _ := OpenReader(path)
	defer r.Close()

	_, ok, err := r.Get([]byte("missing"))
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Error("expected not found for missing key")
	}
}

func TestSSTable_Iterate(t *testing.T) {
	f, err := os.CreateTemp("", "sst_iter_*.sst")
	if err != nil {
		t.Fatal(err)
	}
	path := f.Name()
	f.Close()
	defer os.Remove(path)

	w, _ := NewWriter(path)
	keys := []string{"aaa", "bbb", "ccc", "ddd"}
	for i, k := range keys {
		w.Append([]byte(k), []byte("v"), uint64(i), false)
	}
	w.Finish()

	r, _ := OpenReader(path)
	defer r.Close()

	var got []string
	r.Iterate(func(e Entry) {
		got = append(got, string(e.Key))
	})

	for i, k := range keys {
		if got[i] != k {
			t.Errorf("position %d: got %q want %q", i, got[i], k)
		}
	}
}

func TestSSTable_BloomFilterSkip(t *testing.T) {
	f, err := os.CreateTemp("", "sst_bloom_*.sst")
	if err != nil {
		t.Fatal(err)
	}
	path := f.Name()
	f.Close()
	defer os.Remove(path)

	w, _ := NewWriter(path)
	w.Append([]byte("apple"), []byte("red"), 1, false)
	w.Append([]byte("banana"), []byte("yellow"), 2, false)
	w.Finish()

	r, _ := OpenReader(path)
	defer r.Close()

	if r.MayContainKey([]byte("zzzzzzzzzzzzz")) {
		t.Log("false positive from bloom filter — acceptable but rare")
	}
	if !r.MayContainKey([]byte("apple")) {
		t.Error("bloom filter incorrectly rejected 'apple'")
	}
	if !r.MayContainKey([]byte("banana")) {
		t.Error("bloom filter incorrectly rejected 'banana'")
	}
}
