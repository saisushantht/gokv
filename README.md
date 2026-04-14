
# GoKV

A persistent key-value storage engine written in Go, built on an LSM-tree architecture. I built this to understand how databases like LevelDB and RocksDB work under the hood — from WAL recovery to bloom filter-accelerated reads.

![Go Version](https://img.shields.io/badge/go-1.22+-blue)
![License](https://img.shields.io/badge/license-MIT-green)
![Build](https://img.shields.io/badge/build-passing-brightgreen)

---

## Architecture

```
Write Path:
  Put(key, value)
       │
       ▼
  Write-Ahead Log (WAL)        ← crash safety
       │
       ▼
  Memtable (skip list)         ← in-memory sorted structure
       │
       │ (when full, ~4MB)
       ▼
  SSTable flush to disk        ← immutable sorted file
       │
       │ (when L0 count >= threshold)
       ▼
  Compaction (background)      ← merge, deduplicate, purge tombstones

Read Path:
  Get(key)
       │
       ├─▶ Memtable            ← check first (O(log n))
       │
       ├─▶ SSTable L0          ← bloom filter check → binary search
       ├─▶ SSTable L1          ← bloom filter check → binary search
       └─▶ ...
```

Each SSTable contains:
- **Data block** — sorted key-value pairs in binary encoding
- **Index block** — maps each key to its byte offset in the data block
- **Bloom filter** — probabilistic skip for keys that definitely don't exist
- **Footer** — offsets to the index and bloom blocks

---

## Quick Start

```bash
git clone https://github.com/saisushantht/gokv
cd gokv
go build ./...
go run ./cmd/bench -n 100000
```

---

## API

```go
db, err := gokv.Open(gokv.DefaultConfig("/tmp/mydb"))
if err != nil {
    log.Fatal(err)
}
defer db.Close()

// Write
err = db.Put([]byte("hello"), []byte("world"))

// Read
val, err := db.Get([]byte("hello"))
if errors.Is(err, gokv.ErrKeyNotFound) {
    // key doesn't exist
}

// Delete (writes a tombstone, cleaned up during compaction)
err = db.Delete([]byte("hello"))
```

### Configuration

```go
cfg := gokv.Config{
    Dir:              "/tmp/mydb",
    MemtableSize:     4 * 1024 * 1024, // flush memtable at 4MB
    BloomFPRate:      0.01,             // 1% false positive rate
    L0CompactionSize: 4,                // compact after 4 SSTables
}
```

---

## Benchmarks

Run on Apple M1, Go 1.22, with default configuration:

| Operation         | Ops/sec | Latency (avg) | Allocs/op |
|-------------------|---------|---------------|-----------|
| Put (unique keys) | ~104K   | 9.6 µs        | 13        |
| Put (same key)    | ~224K   | 4.5 µs        | 3         |
| Get (memtable)    | ~4.5M   | 222 ns        | 0         |
| Get (SSTable)     | ~4K     | 244 µs        | 21        |
| Concurrent Put    | ~137K   | 7.3 µs        | 7         |

SSTable reads are slower due to file I/O on each lookup — a block cache would bring this into the hundreds-of-thousands range. That is the next obvious improvement.

---

## Design Decisions

**Skip list over red-black tree for the memtable.**
I chose a skip list because it is simpler to implement correctly in Go, especially for concurrent access. Red-black trees require complex rotations that are easy to get wrong. The probabilistic balancing of a skip list gives O(log n) average case with much less code. LevelDB uses a skip list for the same reason.

**Append-only WAL with no fsync by default.**
Every write goes to the WAL before the memtable. I chose not to fsync on every write because it would cap throughput at the disk IOPS ceiling. A sync mode is straightforward to add for durability-critical workloads.

**Tombstones instead of in-place deletes.**
SSTables are immutable once written. Deleting a key means writing a tombstone marker. The actual removal happens during compaction when the tombstone is the newest version of that key and no older SSTables could still reference it.

**Bloom filters per SSTable.**
For a key that does not exist, without bloom filters every SSTable requires a binary search and a disk seek. With a 1% false positive rate, 99% of those seeks are avoided. The memory cost is roughly 10 bits per key.

**Binary encoding over JSON or protobuf.**
No dependencies, no schema versioning, no overhead. The format is simple enough to decode by hand and fast enough that encoding is never the bottleneck.

---

## Lessons Learned

**sync.RWMutex is not free.** Under high concurrency, even read locks create contention. The right fix is lock striping or a lockless structure, not just switching mutex types.

**bufio.Writer buffers in userspace.** Calling Flush moves data to the OS page cache, not to disk. file.Sync is the only guarantee of durability. These are different things and matter enormously in a storage engine.

**Goroutine leaks are silent.** My first compaction goroutine did not respond to shutdown signals. Tests passed but the goroutine kept running after Close returned. The race detector did not catch it.

**Allocations matter more than I expected.** The Get hot path in the memtable does zero allocations now, but it did not at first. Profiling with pprof showed that converting byte slices to strings for comparisons was creating garbage on every call.

---

## Project Structure

```
gokv/
├── cmd/bench/           # benchmark CLI
├── internal/
│   ├── bloom/           # bloom filter with encode/decode
│   ├── compaction/      # merge-sort compaction with tombstone purging
│   ├── memtable/        # concurrent skip list
│   ├── sstable/         # SSTable writer and reader
│   └── wal/             # write-ahead log with crash recovery
├── gokv.go              # public API
├── config.go            # configuration
├── metrics.go           # Prometheus instrumentation
└── Makefile
```

---

## Running Tests

```bash
make test
make bench
go test -race -v ./internal/wal/...
```
