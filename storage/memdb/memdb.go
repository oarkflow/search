package memdb

import (
	"fmt"
	"sort"
	"strings"

	"github.com/oarkflow/search/storage"

	"github.com/oarkflow/filters"
	maps "github.com/oarkflow/xsync"
)

type MemDB[K storage.Hashable, V any] struct {
	client     maps.IMap[K, V]
	sampleSize int
	comparator storage.Comparator[K]
}

func New[K storage.Hashable, V any](sampleSize int, comparator storage.Comparator[K]) (*MemDB[K, V], error) {
	return &MemDB[K, V]{client: maps.NewMap[K, V](), sampleSize: sampleSize, comparator: comparator}, nil
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
	return uint32(m.client.Size())
}

func (m *MemDB[K, V]) ForEach(fn func(K, V) bool) {
	m.client.ForEach(fn)
}

func (m *MemDB[K, V]) Sample(params storage.SampleParams) (map[string]V, error) {
	sz := m.sampleSize
	if params.Size != 0 {
		sz = params.Size
	}
	value := make(map[string]V)
	var keys []K
	var conditions []filters.Condition
	for _, f := range params.Filters {
		conditions = append(conditions, f)
	}
	// Collect matching keys
	m.client.ForEach(func(key K, val V) bool {
		matched := false
		if params.Rule == nil && params.Filters == nil {
			matched = true
		} else if params.Rule != nil {
			if params.Rule.Match(val) {
				matched = true
			}
		} else if filters.MatchGroup(val, &filters.FilterGroup{Operator: filters.AND, Filters: conditions}) {
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
	m.client.Clear()
	return nil
}
