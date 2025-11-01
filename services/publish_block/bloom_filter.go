package publishblock

import (
	"sync"

	"github.com/bits-and-blooms/bloom/v3"
)

// deduplicator encapsulates the Bloom filter and its lock for safe concurrent access.
type deduplicator struct {
	filter *bloom.BloomFilter
	mu     sync.RWMutex
}

// newDeduplicator creates and initializes a new bloom filter wrapper.
func newDeduplicator(estimatedItems uint, falsePositiveRate float64) *deduplicator {
	return &deduplicator{
		filter: bloom.NewWithEstimates(estimatedItems, falsePositiveRate),
	}
}

// Add safely adds a block ID string to the Bloom filter.
func (d *deduplicator) Add(id string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.filter.AddString(id)
}

// Test safely checks if a block ID string might be in the Bloom filter.
func (d *deduplicator) Test(id string) bool {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.filter.TestString(id)
}
