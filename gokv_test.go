package gokv

import (
	"errors"
	"fmt"
	"os"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

func openTestDB(t *testing.T) (*DB, string) {
	t.Helper()
	dir, err := os.MkdirTemp("", "gokv_test_*")
	if err != nil {
		t.Fatal(err)
	}
	reg := prometheus.NewRegistry()
	db, err := OpenWithRegistry(DefaultConfig(dir), reg)
	if err != nil {
		os.RemoveAll(dir)
		t.Fatal(err)
	}
	return db, dir
}

func TestDB_PutAndGet(t *testing.T) {
	db, dir := openTestDB(t)
	defer os.RemoveAll(dir)
	defer db.Close()

	if err := db.Put([]byte("hello"), []byte("world")); err != nil {
		t.Fatal(err)
	}
	val, err := db.Get([]byte("hello"))
	if err != nil {
		t.Fatal(err)
	}
	if string(val) != "world" {
		t.Errorf("got %q, want %q", val, "world")
	}
}

func TestDB_GetMissingKey(t *testing.T) {
	db, dir := openTestDB(t)
	defer os.RemoveAll(dir)
	defer db.Close()

	_, err := db.Get([]byte("missing"))
	if !errors.Is(err, ErrKeyNotFound) {
		t.Errorf("got %v, want ErrKeyNotFound", err)
	}
}

func TestDB_Delete(t *testing.T) {
	db, dir := openTestDB(t)
	defer os.RemoveAll(dir)
	defer db.Close()

	db.Put([]byte("foo"), []byte("bar"))
	db.Delete([]byte("foo"))

	_, err := db.Get([]byte("foo"))
	if !errors.Is(err, ErrKeyNotFound) {
		t.Errorf("got %v, want ErrKeyNotFound", err)
	}
}

func TestDB_CrashRecovery(t *testing.T) {
	dir, err := os.MkdirTemp("", "gokv_crash_*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	reg := prometheus.NewRegistry()
	db, err := OpenWithRegistry(DefaultConfig(dir), reg)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		db.Put(key, []byte("value"))
	}
	db.Close()

	reg2 := prometheus.NewRegistry()
	db2, err := OpenWithRegistry(DefaultConfig(dir), reg2)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()

	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		_, err := db2.Get(key)
		if err != nil {
			t.Errorf("after recovery: key %q not found: %v", key, err)
		}
	}
}

func TestDB_EmptyKey(t *testing.T) {
	db, dir := openTestDB(t)
	defer os.RemoveAll(dir)
	defer db.Close()

	if err := db.Put([]byte{}, []byte("val")); err == nil {
		t.Error("expected error for empty key")
	}
	if _, err := db.Get([]byte{}); err == nil {
		t.Error("expected error for empty key")
	}
}

func TestDB_FlushAndReadFromSSTable(t *testing.T) {
	dir, err := os.MkdirTemp("", "gokv_flush_*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	cfg := DefaultConfig(dir)
	cfg.MemtableSize = 1

	reg := prometheus.NewRegistry()
	db, err := OpenWithRegistry(cfg, reg)
	if err != nil {
		t.Fatal(err)
	}

	keys := []string{"alpha", "beta", "gamma", "delta"}
	for _, k := range keys {
		if err := db.Put([]byte(k), []byte("value-"+k)); err != nil {
			t.Fatalf("Put(%q): %v", k, err)
		}
	}
	db.Close()

	reg2 := prometheus.NewRegistry()
	db2, err := OpenWithRegistry(cfg, reg2)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()

	for _, k := range keys {
		val, err := db2.Get([]byte(k))
		if err != nil {
			t.Errorf("Get(%q): %v", k, err)
			continue
		}
		want := "value-" + k
		if string(val) != want {
			t.Errorf("Get(%q) = %q, want %q", k, val, want)
		}
	}
}

func TestDB_PersistenceAcrossReopen(t *testing.T) {
	dir, err := os.MkdirTemp("", "gokv_persist_*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	reg := prometheus.NewRegistry()
	db, err := OpenWithRegistry(DefaultConfig(dir), reg)
	if err != nil {
		t.Fatal(err)
	}
	db.Put([]byte("persistent"), []byte("yes"))
	db.Close()

	reg2 := prometheus.NewRegistry()
	db2, err := OpenWithRegistry(DefaultConfig(dir), reg2)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()

	val, err := db2.Get([]byte("persistent"))
	if err != nil {
		t.Fatalf("Get after reopen: %v", err)
	}
	if string(val) != "yes" {
		t.Errorf("got %q, want %q", val, "yes")
	}
}