package gokv

import (
	"fmt"
	"os"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

func openBenchDB(b *testing.B) (*DB, func()) {
	b.Helper()
	dir, err := os.MkdirTemp("", "gokv_bench_*")
	if err != nil {
		b.Fatal(err)
	}
	reg := prometheus.NewRegistry()
	db, err := OpenWithRegistry(DefaultConfig(dir), reg)
	if err != nil {
		os.RemoveAll(dir)
		b.Fatal(err)
	}
	return db, func() {
		db.Close()
		os.RemoveAll(dir)
	}
}

func BenchmarkPut(b *testing.B) {
	db, cleanup := openBenchDB(b)
	defer cleanup()

	key := []byte("benchkey")
	val := []byte("benchvalue-padding-padding-padding")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db.Put(key, val)
	}
}

func BenchmarkPutUniqueKeys(b *testing.B) {
	db, cleanup := openBenchDB(b)
	defer cleanup()

	val := []byte("benchvalue-padding-padding-padding")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		db.Put(key, val)
	}
}

func BenchmarkGet_Memtable(b *testing.B) {
	db, cleanup := openBenchDB(b)
	defer cleanup()

	db.Put([]byte("hotkey"), []byte("value"))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db.Get([]byte("hotkey"))
	}
}

func BenchmarkGet_SSTable(b *testing.B) {
	dir, err := os.MkdirTemp("", "gokv_bench_sst_*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	cfg := DefaultConfig(dir)
	cfg.MemtableSize = 1 // force flush to SSTable

	reg := prometheus.NewRegistry()
	db, err := OpenWithRegistry(cfg, reg)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	db.Put([]byte("hotkey"), []byte("value"))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db.Get([]byte("hotkey"))
	}
}

func BenchmarkConcurrentPut(b *testing.B) {
	db, cleanup := openBenchDB(b)
	defer cleanup()

	val := []byte("value")

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := []byte(fmt.Sprintf("key-%d", i))
			db.Put(key, val)
			i++
		}
	})
}