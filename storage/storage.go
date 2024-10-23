package storage

import (
	"github.com/oarkflow/filters"
)

type Comparator[K any] func(a, b K) int

func Int64Comparator(a, b int64) int {
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}

type SampleParams struct {
	Size      int
	Filters   []*filters.Filter
	Rule      *filters.Rule
	SortOrder string
	SortField string
}

// Store defines the interface for our key-value store
type Store[K comparable, V any] interface {
	Set(key K, value V) error
	Get(key K) (V, bool)
	Del(key K) error
	Len() uint32
	Name() string
	Sample(params SampleParams) (map[string]V, error)
	Close() error
}
