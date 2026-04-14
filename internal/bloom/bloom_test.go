package bloom

import (
	"fmt"
	"testing"
)

func TestBloom_BasicAddAndContains(t *testing.T) {
	f := New(100, 0.01)

	keys := []string{"apple", "banana", "cherry", "date", "elderberry"}
	for _, k := range keys {
		f.Add([]byte(k))
	}

	// All added keys must be found
	for _, k := range keys {
		if !f.MayContain([]byte(k)) {
			t.Errorf("MayContain(%q) = false, want true", k)
		}
	}
}

func TestBloom_DefinitelyNotPresent(t *testing.T) {
	f := New(100, 0.01)
	f.Add([]byte("hello"))

	// A key we never added should come back false
	// (this could theoretically false-positive, but probability is ~1%)
	notAdded := []string{
		"world", "foo", "bar", "baz",
		"zzzzzzzzz", "123456789",
	}
	falsePositives := 0
	for _, k := range notAdded {
		if f.MayContain([]byte(k)) {
			falsePositives++
		}
	}
	// With 1% FP rate and 6 keys, expecting 0 false positives
	// Allow 1 just in case
	if falsePositives > 1 {
		t.Errorf("too many false positives: %d out of %d", falsePositives, len(notAdded))
	}
}

func TestBloom_FalsePositiveRate(t *testing.T) {
	const n = 10000
	const fp = 0.01

	f := New(n, fp)
	for i := 0; i < n; i++ {
		f.Add([]byte(fmt.Sprintf("key-%d", i)))
	}

	// Test with keys that were never inserted
	falsePositives := 0
	const trials = 10000
	for i := n; i < n+trials; i++ {
		if f.MayContain([]byte(fmt.Sprintf("key-%d", i))) {
			falsePositives++
		}
	}

	actualFP := float64(falsePositives) / float64(trials)
	// Allow 2x the configured FP rate as tolerance
	if actualFP > fp*2 {
		t.Errorf("false positive rate %.4f exceeds 2x target %.4f", actualFP, fp)
	}
	t.Logf("false positive rate: %.4f (target: %.4f)", actualFP, fp)
}

func TestBloom_EncodeDecode(t *testing.T) {
	f := New(100, 0.01)
	keys := []string{"alpha", "beta", "gamma"}
	for _, k := range keys {
		f.Add([]byte(k))
	}

	encoded := f.Encode()
	decoded := Decode(encoded)

	if decoded == nil {
		t.Fatal("Decode returned nil")
	}

	// Decoded filter must return true for all added keys
	for _, k := range keys {
		if !decoded.MayContain([]byte(k)) {
			t.Errorf("decoded filter: MayContain(%q) = false, want true", k)
		}
	}
}