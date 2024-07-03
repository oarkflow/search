package storage

import (
	"github.com/oarkflow/filters"
	"github.com/oarkflow/maps"

	"github.com/oarkflow/search/hash"
)

type MemDB[K hash.Hashable, V any] struct {
	client     maps.IMap[K, V]
	sampleSize int
}

func NewMemDB[K hash.Hashable, V any](sampleSize int) (Store[K, V], error) {
	return &MemDB[K, V]{client: maps.New[K, V](), sampleSize: sampleSize}, nil
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

func (m *MemDB[K, V]) Sample(params SampleParams) (map[K]V, error) {
	sz := m.sampleSize
	if params.Size != 0 {
		sz = params.Size
	}
	value := make(map[K]V)
	count := 0
	m.client.ForEach(func(key K, val V) bool {
		if count >= sz {
			return false
		}
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
			var k K
			switch k := any(k).(type) {
			case int64:
				tmp := any(k).(K)
				value[tmp] = val
			case string:
				tmp := any(k).(K)
				value[tmp] = val
			}

			count++
		}
		return true
	})
	return value, nil
}

func (m *MemDB[K, V]) Close() error {
	return nil
}
