package bloom

import (
	"math"
)

// Filter is a space-efficient probabilistic data structure.
// It can tell you definitively if a key is NOT in a set,
// and probably if it IS (with a configurable false positive rate).
type Filter struct {
	bits    []bool
	numHash int
	size    uint
}

// New creates a bloom filter sized for n expected items at false positive rate fp.
func New(n int, fp float64) *Filter {
	size := optimalSize(n, fp)
	numHash := optimalNumHash(size, n)
	return &Filter{
		bits:    make([]bool, size),
		numHash: numHash,
		size:    size,
	}
}

// Add inserts a key into the filter.
func (f *Filter) Add(key []byte) {
	for i := 0; i < f.numHash; i++ {
		pos := f.hashAt(key, i)
		f.bits[pos] = true
	}
}

// MayContain returns false if key is definitely not in the set.
// Returns true if key is probably in the set.
func (f *Filter) MayContain(key []byte) bool {
	for i := 0; i < f.numHash; i++ {
		pos := f.hashAt(key, i)
		if !f.bits[pos] {
			return false
		}
	}
	return true
}

// hashAt computes the i-th hash position for a key.
// Uses double hashing: h(i) = h1(key) + i*h2(key)
func (f *Filter) hashAt(key []byte, i int) uint {
	h1, h2 := fnv(key)
	return (h1 + uint(i)*h2) % f.size
}

// fnv computes two independent hashes using FNV-1a variants.
func fnv(key []byte) (uint, uint) {
	const (
		offset32 = 2166136261
		prime32  = 16777619
	)
	h1 := uint(offset32)
	h2 := uint(offset32 ^ 0xdeadbeef)

	for _, b := range key {
		h1 ^= uint(b)
		h1 *= prime32
		h2 ^= uint(b)
		h2 *= prime32 ^ 0x1234567
	}
	return h1, h2
}

// optimalSize computes the optimal bit array size for n items at false positive rate fp.
// Formula: m = -n*ln(fp) / (ln(2)^2)
func optimalSize(n int, fp float64) uint {
	m := -float64(n) * math.Log(fp) / (math.Log(2) * math.Log(2))
	return uint(math.Ceil(m))
}

// optimalNumHash computes the optimal number of hash functions.
// Formula: k = (m/n) * ln(2)
func optimalNumHash(m uint, n int) int {
	k := float64(m) / float64(n) * math.Log(2)
	return int(math.Ceil(k))
}

// Encode serializes the filter to bytes for storage in an SSTable.
// Format: [4 bytes: numHash] [4 bytes: size] [size bytes: bits]
func (f *Filter) Encode() []byte {
	buf := make([]byte, 8+len(f.bits))

	// Write numHash (4 bytes)
	buf[0] = byte(f.numHash)
	buf[1] = byte(f.numHash >> 8)
	buf[2] = byte(f.numHash >> 16)
	buf[3] = byte(f.numHash >> 24)

	// Write size (4 bytes)
	buf[4] = byte(f.size)
	buf[5] = byte(f.size >> 8)
	buf[6] = byte(f.size >> 16)
	buf[7] = byte(f.size >> 24)

	// Write bits
	for i, b := range f.bits {
		if b {
			buf[8+i] = 1
		}
	}
	return buf
}

// Decode deserializes a bloom filter from bytes.
func Decode(data []byte) *Filter {
	if len(data) < 8 {
		return nil
	}

	numHash := int(data[0]) | int(data[1])<<8 | int(data[2])<<16 | int(data[3])<<24
	size := uint(data[4]) | uint(data[5])<<8 | uint(data[6])<<16 | uint(data[7])<<24

	bits := make([]bool, size)
	for i := uint(0); i < size && 8+int(i) < len(data); i++ {
		bits[i] = data[8+i] == 1
	}

	return &Filter{
		bits:    bits,
		numHash: numHash,
		size:    size,
	}
}