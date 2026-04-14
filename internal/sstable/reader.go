package sstable

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"

	"github.com/saisushantht/gokv/internal/bloom"
)

// Entry is a single record read from an SSTable.
type Entry struct {
	Key       []byte
	Value     []byte
	Timestamp uint64
	Tombstone bool
}

// Reader reads entries from an SSTable file.
type Reader struct {
	file   *os.File
	index  []indexEntry
	filter *bloom.Filter
	count  uint64
}

// OpenReader opens an SSTable for reading.
func OpenReader(path string) (*Reader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("sstable: opening file: %w", err)
	}
	r := &Reader{file: f}
	if err := r.loadFooter(); err != nil {
		f.Close()
		return nil, err
	}
	return r, nil
}

// loadFooter reads the footer, then loads the index and bloom filter.
// Footer is the last 24 bytes: [8 index start][8 bloom start][8 count]
func (r *Reader) loadFooter() error {
	if _, err := r.file.Seek(-24, io.SeekEnd); err != nil {
		return fmt.Errorf("sstable: seeking to footer: %w", err)
	}

	var indexStart, bloomStart, count uint64
	if err := binary.Read(r.file, binary.LittleEndian, &indexStart); err != nil {
		return fmt.Errorf("sstable: reading index start: %w", err)
	}
	if err := binary.Read(r.file, binary.LittleEndian, &bloomStart); err != nil {
		return fmt.Errorf("sstable: reading bloom start: %w", err)
	}
	if err := binary.Read(r.file, binary.LittleEndian, &count); err != nil {
		return fmt.Errorf("sstable: reading count: %w", err)
	}
	r.count = count

	if err := r.loadIndex(indexStart, count); err != nil {
		return err
	}
	if err := r.loadBloom(bloomStart); err != nil {
		return err
	}
	return nil
}

func (r *Reader) loadIndex(indexStart, count uint64) error {
	if _, err := r.file.Seek(int64(indexStart), io.SeekStart); err != nil {
		return fmt.Errorf("sstable: seeking to index: %w", err)
	}
	r.index = make([]indexEntry, 0, count)
	for i := uint64(0); i < count; i++ {
		var keyLen uint32
		if err := binary.Read(r.file, binary.LittleEndian, &keyLen); err != nil {
			return fmt.Errorf("sstable: reading index key length: %w", err)
		}
		key := make([]byte, keyLen)
		if _, err := io.ReadFull(r.file, key); err != nil {
			return fmt.Errorf("sstable: reading index key: %w", err)
		}
		var offset uint64
		if err := binary.Read(r.file, binary.LittleEndian, &offset); err != nil {
			return fmt.Errorf("sstable: reading index offset: %w", err)
		}
		r.index = append(r.index, indexEntry{key: key, offset: offset})
	}
	return nil
}

func (r *Reader) loadBloom(bloomStart uint64) error {
	if _, err := r.file.Seek(int64(bloomStart), io.SeekStart); err != nil {
		return fmt.Errorf("sstable: seeking to bloom: %w", err)
	}
	var bloomLen uint32
	if err := binary.Read(r.file, binary.LittleEndian, &bloomLen); err != nil {
		return fmt.Errorf("sstable: reading bloom length: %w", err)
	}
	bloomData := make([]byte, bloomLen)
	if _, err := io.ReadFull(r.file, bloomData); err != nil {
		return fmt.Errorf("sstable: reading bloom data: %w", err)
	}
	r.filter = bloom.Decode(bloomData)
	return nil
}

// MayContainKey returns false if key is definitely not in this SSTable.
// Allows callers to skip the file entirely without a disk seek.
func (r *Reader) MayContainKey(key []byte) bool {
	if r.filter == nil {
		return true // no filter, assume it might be there
	}
	return r.filter.MayContain(key)
}

// Get looks up a key using the index then seeks directly to the entry.
func (r *Reader) Get(key []byte) (Entry, bool, error) {
	if !r.MayContainKey(key) {
		return Entry{}, false, nil
	}

	// Binary search the index
	lo, hi := 0, len(r.index)-1
	pos := -1
	for lo <= hi {
		mid := (lo + hi) / 2
		cmp := string(r.index[mid].key)
		if cmp == string(key) {
			pos = mid
			break
		} else if cmp < string(key) {
			lo = mid + 1
		} else {
			hi = mid - 1
		}
	}
	if pos == -1 {
		return Entry{}, false, nil
	}

	if _, err := r.file.Seek(int64(r.index[pos].offset), io.SeekStart); err != nil {
		return Entry{}, false, fmt.Errorf("sstable: seeking to entry: %w", err)
	}
	entry, err := readEntry(r.file)
	if err != nil {
		return Entry{}, false, err
	}
	return entry, true, nil
}

// Iterate calls fn for every entry in sorted order.
func (r *Reader) Iterate(fn func(e Entry)) error {
	if _, err := r.file.Seek(0, io.SeekStart); err != nil {
		return err
	}
	for i := uint64(0); i < r.count; i++ {
		e, err := readEntry(r.file)
		if err != nil {
			return err
		}
		fn(e)
	}
	return nil
}

// Close closes the SSTable file.
func (r *Reader) Close() error {
	return r.file.Close()
}

// readEntry reads one data entry from the current file position.
func readEntry(f *os.File) (Entry, error) {
	var keyLen uint32
	if err := binary.Read(f, binary.LittleEndian, &keyLen); err != nil {
		return Entry{}, fmt.Errorf("sstable: reading key length: %w", err)
	}
	key := make([]byte, keyLen)
	if _, err := io.ReadFull(f, key); err != nil {
		return Entry{}, fmt.Errorf("sstable: reading key: %w", err)
	}
	var valueLen uint32
	if err := binary.Read(f, binary.LittleEndian, &valueLen); err != nil {
		return Entry{}, fmt.Errorf("sstable: reading value length: %w", err)
	}
	value := make([]byte, valueLen)
	if _, err := io.ReadFull(f, value); err != nil {
		return Entry{}, fmt.Errorf("sstable: reading value: %w", err)
	}
	var ts uint64
	if err := binary.Read(f, binary.LittleEndian, &ts); err != nil {
		return Entry{}, fmt.Errorf("sstable: reading timestamp: %w", err)
	}
	tb := make([]byte, 1)
	if _, err := io.ReadFull(f, tb); err != nil {
		return Entry{}, fmt.Errorf("sstable: reading tombstone: %w", err)
	}
	return Entry{
		Key:       key,
		Value:     value,
		Timestamp: ts,
		Tombstone: tb[0] == 0x01,
	}, nil
}