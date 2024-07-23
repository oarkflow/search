package storage

import (
	"fmt"
	"sort"
	"strings"
	"unsafe"

	"github.com/oarkflow/filters"
	"github.com/oarkflow/maps"
	"golang.org/x/exp/constraints"
)

func Int64Comparator(a, b int64) int {
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}

type hashable interface {
	constraints.Integer | constraints.Float | constraints.Complex | ~string | uintptr | unsafe.Pointer
}

type Comparator[K any] func(a, b K) int

type MemDB[K hashable, V any] struct {
	client     maps.IMap[K, V]
	sampleSize int
	comparator Comparator[K]
}

func NewMemDB[K hashable, V any](sampleSize int, comparator Comparator[K]) (*MemDB[K, V], error) {
	return &MemDB[K, V]{client: maps.New[K, V](), sampleSize: sampleSize, comparator: comparator}, nil
}

func (m *MemDB[K, V]) Set(key K, value V) error {
	m.client.Set(key, value)
	return nil
}

func (m *MemDB[K, V]) Name() string {
	return "memdb"
}

func (m *MemDB[K, V]) Get(key K) (V, bool) {
	return m.client.Get(key)
}

func (m *MemDB[K, V]) Del(key K) error {
	m.client.Del(key)
	return nil
}

func (m *MemDB[K, V]) Len() uint32 {
	return uint32(m.client.Len())
}

func (m *MemDB[K, V]) Sample(params SampleParams) (map[string]V, error) {
	sz := m.sampleSize
	if params.Size != 0 {
		sz = params.Size
	}
	value := make(map[string]V)
	var keys []K

	// Collect matching keys
	m.client.ForEach(func(key K, val V) bool {
		matched := false
		if params.Sequence == nil && params.Filters == nil {
			matched = true
		} else if params.Sequence != nil {
			if params.Sequence.Match(val) {
				matched = true
			}
		} else if filters.MatchGroup(val, &filters.FilterGroup{Operator: filters.AND, Filters: params.Filters}) {
			matched = true
		}
		if matched {
			keys = append(keys, key)
		}
		return true
	})
	if params.Sort == "" || strings.ToLower(params.Sort) == "asc" {
		sort.Slice(keys, func(i, j int) bool {
			return m.comparator(keys[i], keys[j]) < 0
		})
	} else if strings.ToLower(params.Sort) == "desc" {
		sort.Slice(keys, func(i, j int) bool {
			return m.comparator(keys[i], keys[j]) > 0
		})
	}

	// Collect values for sorted keys
	count := 0
	for _, key := range keys {
		if count >= sz {
			break
		}
		val, _ := m.client.Get(key)
		tmp := fmt.Sprint(key)
		value[tmp] = val
		count++
	}

	return value, nil
}

func (m *MemDB[K, V]) Close() error {
	return nil
}
