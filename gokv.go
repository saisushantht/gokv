package gokv

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/saisushantht/gokv/internal/compaction"
	"github.com/saisushantht/gokv/internal/memtable"
	"github.com/saisushantht/gokv/internal/sstable"
	"github.com/saisushantht/gokv/internal/wal"
)

// ErrKeyNotFound is returned when a key does not exist in the database.
var ErrKeyNotFound = errors.New("gokv: key not found")

// DB is the main storage engine handle.
type DB struct {
	config    Config
	wal       *wal.WAL
	mem       *memtable.Memtable
	mu        sync.RWMutex
	closed    bool
	sstables  []string
	sstMu     sync.RWMutex
	ts        atomic.Uint64
	nextSeq   atomic.Uint64
	compactC  chan struct{}
	closeC    chan struct{}
	compactWg sync.WaitGroup
	m         *metrics
}

// Open opens or creates a GoKV database at the directory in config.
func Open(cfg Config) (*DB, error) {
	return OpenWithRegistry(cfg, prometheus.DefaultRegisterer)
}

// OpenWithRegistry opens a DB and registers metrics with a custom Prometheus registry.
// Use this in tests to avoid duplicate metric registration panics.
func OpenWithRegistry(cfg Config, reg prometheus.Registerer) (*DB, error) {
	if cfg.Dir == "" {
		return nil, fmt.Errorf("gokv: config.Dir must not be empty")
	}

	if err := os.MkdirAll(cfg.Dir, 0755); err != nil {
		return nil, fmt.Errorf("gokv: creating data dir: %w", err)
	}

	walPath := filepath.Join(cfg.Dir, "wal.log")
	w, err := wal.Open(walPath)
	if err != nil {
		return nil, fmt.Errorf("gokv: opening WAL: %w", err)
	}

	db := &DB{
		config:   cfg,
		wal:      w,
		mem:      memtable.New(),
		compactC: make(chan struct{}, 1),
		closeC:   make(chan struct{}),
		m:        newMetrics(reg),
	}

	if err := db.loadSSTables(); err != nil {
		w.Close()
		return nil, fmt.Errorf("gokv: loading sstables: %w", err)
	}

	if err := db.replay(); err != nil {
		w.Close()
		return nil, fmt.Errorf("gokv: replaying WAL: %w", err)
	}

	db.compactWg.Add(1)
	go db.compactionLoop()

	return db, nil
}

// compactionLoop runs in the background and compacts SSTables when signaled.
func (db *DB) compactionLoop() {
	defer db.compactWg.Done()
	c := compaction.NewCompactor(db.config.Dir, db.config.L0CompactionSize)

	for {
		select {
		case <-db.closeC:
			return
		case <-db.compactC:
			db.sstMu.RLock()
			paths := make([]string, len(db.sstables))
			copy(paths, db.sstables)
			db.sstMu.RUnlock()

			start := time.Now()
			newPaths, err := c.MaybeCompact(paths)
			if err != nil {
				continue
			}
			db.m.compactionDuration.Observe(time.Since(start).Seconds())

			db.sstMu.Lock()
			db.sstables = newPaths
			db.m.sstableCount.Set(float64(len(newPaths)))
			db.sstMu.Unlock()
		}
	}
}

// triggerCompaction sends a non-blocking signal to the compaction goroutine.
func (db *DB) triggerCompaction() {
	select {
	case db.compactC <- struct{}{}:
	default:
	}
}

// loadSSTables scans the data directory for existing SSTable files.
func (db *DB) loadSSTables() error {
	entries, err := os.ReadDir(db.config.Dir)
	if err != nil {
		return err
	}

	var paths []string
	var maxSeq uint64

	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".sst") {
			continue
		}
		path := filepath.Join(db.config.Dir, e.Name())
		paths = append(paths, path)

		name := strings.TrimSuffix(e.Name(), ".sst")
		seq, err := strconv.ParseUint(name, 10, 64)
		if err == nil && seq > maxSeq {
			maxSeq = seq
		}
	}

	sort.Slice(paths, func(i, j int) bool {
		return paths[i] > paths[j]
	})

	db.sstables = paths
	db.nextSeq.Store(maxSeq)
	return nil
}

// replay reads the WAL and reconstructs the memtable.
func (db *DB) replay() error {
	var maxTS uint64
	db.wal.Replay(func(key, value []byte, ts uint64, tombstone bool) {
		if tombstone {
			db.mem.Delete(key, ts)
		} else {
			db.mem.Put(key, value, ts)
		}
		if ts > maxTS {
			maxTS = ts
		}
	})
	db.ts.Store(maxTS)
	return nil
}

// nextTS returns a monotonically increasing timestamp.
func (db *DB) nextTS() uint64 {
	return db.ts.Add(1)
}

// Put writes a key-value pair to the database.
func (db *DB) Put(key, value []byte) error {
	if len(key) == 0 {
		return fmt.Errorf("gokv: key must not be empty")
	}

	start := time.Now()
	defer func() {
		db.m.writesTotal.Inc()
		db.m.writeLatency.Observe(time.Since(start).Seconds())
		db.m.memtableSize.Set(float64(db.mem.Size()))
	}()

	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed {
		return fmt.Errorf("gokv: database is closed")
	}

	ts := db.nextTS()

	if err := db.wal.Append(key, value, ts, false); err != nil {
		return fmt.Errorf("gokv: writing to WAL: %w", err)
	}

	db.mem.Put(key, value, ts)

	if db.mem.Size() >= db.config.MemtableSize {
		if err := db.flushMemtable(); err != nil {
			return fmt.Errorf("gokv: flushing memtable: %w", err)
		}
		db.triggerCompaction()
	}

	return nil
}

// Get retrieves the value for a key.
func (db *DB) Get(key []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, fmt.Errorf("gokv: key must not be empty")
	}

	start := time.Now()
	defer func() {
		db.m.readsTotal.Inc()
		db.m.readLatency.Observe(time.Since(start).Seconds())
	}()

	db.mu.RLock()
	defer db.mu.RUnlock()

	if db.closed {
		return nil, fmt.Errorf("gokv: database is closed")
	}

	val, ok := db.mem.Get(key)
	if ok {
		if val == nil {
			return nil, ErrKeyNotFound
		}
		return val, nil
	}

	db.sstMu.RLock()
	paths := make([]string, len(db.sstables))
	copy(paths, db.sstables)
	db.sstMu.RUnlock()

	for _, path := range paths {
		r, err := sstable.OpenReader(path)
		if err != nil {
			return nil, fmt.Errorf("gokv: opening sstable: %w", err)
		}

		// Count bloom filter hit/miss
		if r.MayContainKey(key) {
			db.m.bloomHits.Inc()
		} else {
			db.m.bloomMisses.Inc()
		}

		entry, found, err := r.Get(key)
		r.Close()

		if err != nil {
			return nil, fmt.Errorf("gokv: reading sstable: %w", err)
		}
		if found {
			if entry.Tombstone {
				return nil, ErrKeyNotFound
			}
			return entry.Value, nil
		}
	}

	return nil, ErrKeyNotFound
}

// Delete removes a key by writing a tombstone.
func (db *DB) Delete(key []byte) error {
	if len(key) == 0 {
		return fmt.Errorf("gokv: key must not be empty")
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed {
		return fmt.Errorf("gokv: database is closed")
	}

	ts := db.nextTS()

	if err := db.wal.Append(key, nil, ts, true); err != nil {
		return fmt.Errorf("gokv: writing tombstone to WAL: %w", err)
	}

	db.mem.Delete(key, ts)
	return nil
}

// flushMemtable writes the current memtable to a new SSTable file.
// Caller must hold db.mu write lock.
func (db *DB) flushMemtable() error {
	seq := db.nextSeq.Add(1)
	filename := fmt.Sprintf("%06d.sst", seq)
	path := filepath.Join(db.config.Dir, filename)

	w, err := sstable.NewWriter(path)
	if err != nil {
		return fmt.Errorf("creating sstable writer: %w", err)
	}

	db.mem.Iterate(func(key, value []byte, ts uint64, tombstone bool) {
		w.Append(key, value, ts, tombstone)
	})

	if err := w.Finish(); err != nil {
		return fmt.Errorf("finishing sstable: %w", err)
	}

	db.sstMu.Lock()
	db.sstables = append([]string{path}, db.sstables...)
	db.m.sstableCount.Set(float64(len(db.sstables)))
	db.sstMu.Unlock()

	db.mem = memtable.New()
	return nil
}

// Close flushes remaining data, stops background goroutines, and closes the DB.
func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed {
		return nil
	}
	db.closed = true

	close(db.closeC)
	db.compactWg.Wait()

	if db.mem.Size() > 0 {
		if err := db.flushMemtable(); err != nil {
			return fmt.Errorf("gokv: flushing on close: %w", err)
		}
	}

	return db.wal.Close()
}