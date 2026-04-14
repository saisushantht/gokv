package gokv

// Config holds configuration for the storage engine.
type Config struct {
	Dir              string  // data directory for WAL and SSTables
	MemtableSize     int64   // bytes before memtable is flushed (default: 4MB)
	BloomFPRate      float64 // bloom filter false positive rate (default: 0.01)
	L0CompactionSize int     // number of SSTables before compaction (default: 4)
}

// DefaultConfig returns a Config with sensible defaults.
func DefaultConfig(dir string) Config {
	return Config{
		Dir:              dir,
		MemtableSize:     4 * 1024 * 1024, // 4MB
		BloomFPRate:      0.01,
		L0CompactionSize: 4,
	}
}