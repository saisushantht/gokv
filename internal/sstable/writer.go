package sstable

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"os"

	"github.com/saisushantht/gokv/internal/bloom"
)

// On-disk format:
//
// [Data Block]
//   repeated:
//     [4 bytes: key length] [key bytes]
//     [4 bytes: value length] [value bytes]
//     [8 bytes: timestamp]
//     [1 byte: tombstone]
//
// [Index Block]
//   repeated:
//     [4 bytes: key length] [key bytes]
//     [8 bytes: offset in data block]
//
// [Bloom Block]
//     [4 bytes: bloom data length] [bloom data bytes]
//
// [Footer]
//     [8 bytes: index block start offset]
//     [8 bytes: bloom block start offset]
//     [8 bytes: entry count]

// Writer writes a sorted sequence of entries to an SSTable file.
type Writer struct {
	file   *os.File
	writer *bufio.Writer
	index  []indexEntry
	filter *bloom.Filter
	offset uint64
	count  uint64
}

type indexEntry struct {
	key    []byte
	offset uint64
}

// NewWriter creates a new SSTable writer at the given path.
// expectedKeys is used to size the bloom filter.
func NewWriter(path string) (*Writer, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("sstable: creating file: %w", err)
	}
	return &Writer{
		file:   f,
		writer: bufio.NewWriter(f),
		filter: bloom.New(10000, 0.01), // sized for typical memtable
	}, nil
}

// Append writes one entry. Keys must be supplied in sorted ascending order.
func (w *Writer) Append(key, value []byte, ts uint64, tombstone bool) error {
	w.index = append(w.index, indexEntry{
		key:    append([]byte{}, key...),
		offset: w.offset,
	})

	w.filter.Add(key)

	if err := binary.Write(w.writer, binary.LittleEndian, uint32(len(key))); err != nil {
		return err
	}
	if _, err := w.writer.Write(key); err != nil {
		return err
	}
	if err := binary.Write(w.writer, binary.LittleEndian, uint32(len(value))); err != nil {
		return err
	}
	if _, err := w.writer.Write(value); err != nil {
		return err
	}
	if err := binary.Write(w.writer, binary.LittleEndian, ts); err != nil {
		return err
	}
	tb := byte(0x00)
	if tombstone {
		tb = 0x01
	}
	if err := w.writer.WriteByte(tb); err != nil {
		return err
	}

	w.offset += uint64(4 + len(key) + 4 + len(value) + 8 + 1)
	w.count++
	return nil
}

// Finish writes the index block, bloom block, and footer, then closes the file.
func (w *Writer) Finish() error {
	indexStart := w.offset

	// Write index block
	for _, ie := range w.index {
		if err := binary.Write(w.writer, binary.LittleEndian, uint32(len(ie.key))); err != nil {
			return err
		}
		if _, err := w.writer.Write(ie.key); err != nil {
			return err
		}
		if err := binary.Write(w.writer, binary.LittleEndian, ie.offset); err != nil {
			return err
		}
		w.offset += uint64(4 + len(ie.key) + 8)
	}

	bloomStart := w.offset

	// Write bloom block
	bloomData := w.filter.Encode()
	if err := binary.Write(w.writer, binary.LittleEndian, uint32(len(bloomData))); err != nil {
		return err
	}
	if _, err := w.writer.Write(bloomData); err != nil {
		return err
	}

	// Write footer: index start + bloom start + count
	if err := binary.Write(w.writer, binary.LittleEndian, indexStart); err != nil {
		return err
	}
	if err := binary.Write(w.writer, binary.LittleEndian, bloomStart); err != nil {
		return err
	}
	if err := binary.Write(w.writer, binary.LittleEndian, w.count); err != nil {
		return err
	}

	if err := w.writer.Flush(); err != nil {
		return fmt.Errorf("sstable: flushing: %w", err)
	}
	return w.file.Close()
}