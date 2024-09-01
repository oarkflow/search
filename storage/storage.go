package storage

import (
	"github.com/oarkflow/filters"

	"github.com/oarkflow/search/janitor"
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

func StringComparator(a, b string) int {
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}

type SampleParams struct {
	Size    int
	Filters []*filters.Filter
	Rule    *filters.Rule
	Sort    string
}

// Store defines the interface for our key-value store
type Store[K comparable, V any] interface {
	janitor.DataSource[K, V]
	Len() uint32
	Name() string
	Sample(params SampleParams) (map[string]V, error)
	Close() error
}
