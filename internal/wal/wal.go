package wal

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sync"
)

// WAL is a write-ahead log that records every write before it goes to memory.
// Format of each entry:
// [4 bytes: key length] [key bytes] [4 bytes: value length] [value bytes] [8 bytes: timestamp] [1 byte: tombstone]
type WAL struct {
	mu     sync.Mutex
	file   *os.File
	writer *bufio.Writer
}

// Open opens or creates a WAL file at the given path.
func Open(path string) (*WAL, error) {
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("wal: opening file: %w", err)
	}

	return &WAL{
		file:   file,
		writer: bufio.NewWriter(file),
	}, nil
}

// Append writes one entry to the WAL.
func (w *WAL) Append(key, value []byte, ts uint64, tombstone bool) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Write key length + key
	if err := binary.Write(w.writer, binary.LittleEndian, uint32(len(key))); err != nil {
		return fmt.Errorf("wal: writing key length: %w", err)
	}
	if _, err := w.writer.Write(key); err != nil {
		return fmt.Errorf("wal: writing key: %w", err)
	}

	// Write value length + value
	if err := binary.Write(w.writer, binary.LittleEndian, uint32(len(value))); err != nil {
		return fmt.Errorf("wal: writing value length: %w", err)
	}
	if _, err := w.writer.Write(value); err != nil {
		return fmt.Errorf("wal: writing value: %w", err)
	}

	// Write timestamp
	if err := binary.Write(w.writer, binary.LittleEndian, ts); err != nil {
		return fmt.Errorf("wal: writing timestamp: %w", err)
	}

	// Write tombstone flag
	tb := byte(0x00)
	if tombstone {
		tb = 0x01
	}
	if err := w.writer.WriteByte(tb); err != nil {
		return fmt.Errorf("wal: writing tombstone: %w", err)
	}

	// Flush the bufio.Writer to the OS
	if err := w.writer.Flush(); err != nil {
		return fmt.Errorf("wal: flushing: %w", err)
	}

	return nil
}

// Replay reads all entries from the WAL and calls fn for each valid entry.
// Used during crash recovery to reconstruct the memtable.
func (w *WAL) Replay(fn func(key, value []byte, ts uint64, tombstone bool)) error {
	// Open a separate read handle — we can't seek on our append-only write handle
	file, err := os.Open(w.file.Name())
	if err != nil {
		return fmt.Errorf("wal: opening for replay: %w", err)
	}
	defer file.Close()

	reader := bufio.NewReader(file)

	for {
		// Read key length
		var keyLen uint32
		if err := binary.Read(reader, binary.LittleEndian, &keyLen); err != nil {
			if err == io.EOF {
				break // clean end of file
			}
			// Partial entry at end — process died mid-write, stop here
			break
		}

		// Read key
		key := make([]byte, keyLen)
		if _, err := io.ReadFull(reader, key); err != nil {
			break // partial entry, stop
		}

		// Read value length
		var valueLen uint32
		if err := binary.Read(reader, binary.LittleEndian, &valueLen); err != nil {
			break
		}

		// Read value
		value := make([]byte, valueLen)
		if _, err := io.ReadFull(reader, value); err != nil {
			break
		}

		// Read timestamp
		var ts uint64
		if err := binary.Read(reader, binary.LittleEndian, &ts); err != nil {
			break
		}

		// Read tombstone flag
		tb, err := reader.ReadByte()
		if err != nil {
			break
		}

		fn(key, value, ts, tb == 0x01)
	}

	return nil
}

// Close flushes and closes the WAL file.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.writer.Flush(); err != nil {
		return fmt.Errorf("wal: closing flush: %w", err)
	}
	if err := w.file.Close(); err != nil {
		return fmt.Errorf("wal: closing file: %w", err)
	}
	return nil
}