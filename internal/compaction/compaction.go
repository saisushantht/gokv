package compaction

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/saisushantht/gokv/internal/sstable"
)

// Entry represents a key-value record during merge.
type entry struct {
	key       []byte
	value     []byte
	timestamp uint64
	tombstone bool
}

// MergeSSTables merges multiple SSTable files into a single new SSTable.
// Input files must each be internally sorted. Output is sorted and deduplicated.
// Latest timestamp wins for duplicate keys. Tombstones are dropped if purge=true.
func MergeSSTables(inputPaths []string, outputPath string, purgeTombstones bool) error {
	if len(inputPaths) == 0 {
		return fmt.Errorf("compaction: no input files")
	}

	// Open all readers
	readers := make([]*sstable.Reader, 0, len(inputPaths))
	for _, p := range inputPaths {
		r, err := sstable.OpenReader(p)
		if err != nil {
			for _, rr := range readers {
				rr.Close()
			}
			return fmt.Errorf("compaction: opening %q: %w", p, err)
		}
		readers = append(readers, r)
	}
	defer func() {
		for _, r := range readers {
			r.Close()
		}
	}()

	// Collect all entries from all SSTables into memory
	// For large datasets this would use a heap — fine for our scale
	var all []entry
	for _, r := range readers {
		r.Iterate(func(e sstable.Entry) {
			all = append(all, entry{
				key:       e.Key,
				value:     e.Value,
				timestamp: e.Timestamp,
				tombstone: e.Tombstone,
			})
		})
	}

	// Sort by key asc, then by timestamp desc (newest first)
	sort.SliceStable(all, func(i, j int) bool {
		ki, kj := string(all[i].key), string(all[j].key)
		if ki != kj {
			return ki < kj
		}
		return all[i].timestamp > all[j].timestamp
	})

	// Deduplicate: for each key, keep only the entry with the highest timestamp
	deduped := make([]entry, 0, len(all))
	for i, e := range all {
		if i > 0 && string(e.key) == string(all[i-1].key) {
			continue // older version of same key, skip
		}
		if purgeTombstones && e.tombstone {
			continue // safe to drop tombstone during major compaction
		}
		deduped = append(deduped, e)
	}

	// Write merged output
	w, err := sstable.NewWriter(outputPath)
	if err != nil {
		return fmt.Errorf("compaction: creating output: %w", err)
	}

	for _, e := range deduped {
		if err := w.Append(e.key, e.value, e.timestamp, e.tombstone); err != nil {
			return fmt.Errorf("compaction: writing entry: %w", err)
		}
	}

	if err := w.Finish(); err != nil {
		return fmt.Errorf("compaction: finishing output: %w", err)
	}

	return nil
}

// Compactor manages background compaction of SSTable levels.
type Compactor struct {
	dir             string
	l0Threshold     int // number of L0 SSTables before compaction triggers
}

// NewCompactor creates a new compactor.
func NewCompactor(dir string, l0Threshold int) *Compactor {
	return &Compactor{
		dir:         dir,
		l0Threshold: l0Threshold,
	}
}

// MaybeCompact runs compaction if the number of SSTables exceeds the threshold.
// Returns the new list of SSTable paths after compaction (newest first).
func (c *Compactor) MaybeCompact(paths []string) ([]string, error) {
	if len(paths) < c.l0Threshold {
		return paths, nil
	}

	// Merge all current SSTables into one
	seq := nextSeq(paths)
	outputPath := filepath.Join(c.dir, fmt.Sprintf("%06d.sst", seq))

	// Oldest to newest for correct timestamp ordering during merge
	reversed := make([]string, len(paths))
	for i, p := range paths {
		reversed[len(paths)-1-i] = p
	}

	if err := MergeSSTables(reversed, outputPath, true); err != nil {
		return paths, fmt.Errorf("compaction: merge failed: %w", err)
	}

	// Remove old SSTable files
	for _, p := range paths {
		if err := os.Remove(p); err != nil {
			return paths, fmt.Errorf("compaction: removing old sstable %q: %w", p, err)
		}
	}

	return []string{outputPath}, nil
}

// nextSeq returns a sequence number higher than any in the existing paths.
func nextSeq(paths []string) uint64 {
	var max uint64
	for _, p := range paths {
		base := filepath.Base(p)
		var seq uint64
		fmt.Sscanf(base, "%d.sst", &seq)
		if seq > max {
			max = seq
		}
	}
	return max + 1
}