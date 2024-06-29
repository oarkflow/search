package storage

import (
	"github.com/oarkflow/filters"
)

type SampleParams struct {
	Size     int
	Filters  []*filters.Filter
	Sequence *filters.Sequence
}

// Store defines the interface for our key-value store
type Store[K comparable, V any] interface {
	Set(key K, value V) error
	Get(key K) (V, bool)
	Del(key K) error
	Len() uint32
	Sample(params SampleParams) (map[string]V, error)
	Close() error
}
