package storage

import (
	"github.com/oarkflow/filters"
)

type SampleParams struct {
	Size    int
	Filters []*filters.Filter
	Rule    *filters.Rule
	Sort    string
}

// Store defines the interface for our key-value store
type Store[K comparable, V any] interface {
	Set(key K, value V) error
	Get(key K) (V, bool)
	Del(key K) error
	Len() uint32
	Name() string
	Sample(params SampleParams) (map[string]V, error)
	ForEach(func(K, V) bool)
	Close() error
}
