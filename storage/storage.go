package storage

import (
	"github.com/oarkflow/filters"

	"github.com/oarkflow/search/hash"
)

type SampleParams struct {
	Size     int
	Filters  []*filters.Filter
	Sequence *filters.Sequence
}

// Store defines the interface for our key-value store
type Store[K hash.Hashable, V any] interface {
	Set(key K, value V) error
	Get(key K) (V, bool)
	Del(key K) error
	Len() uint32
	Name() string
	Sample(params SampleParams) (map[K]V, error)
	Close() error
}
