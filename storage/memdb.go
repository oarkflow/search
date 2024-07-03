package storage

import (
	"fmt"
	"unsafe"

	"github.com/oarkflow/filters"
	"github.com/oarkflow/maps"
	"golang.org/x/exp/constraints"
)

type hashable interface {
	constraints.Integer | constraints.Float | constraints.Complex | ~string | uintptr | unsafe.Pointer
}

type MemDB[K hashable, V any] struct {
	client     maps.IMap[K, V]
	sampleSize int
}

func NewMemDB[K hashable, V any](sampleSize int) (Store[K, V], error) {
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

func (m *MemDB[K, V]) Sample(params SampleParams) (map[string]V, error) {
	sz := m.sampleSize
	if params.Size != 0 {
		sz = params.Size
	}
	value := make(map[string]V)
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
			tmp := fmt.Sprint(key)
			value[tmp] = val
			count++
		}
		return true
	})
	return value, nil
}

func (m *MemDB[K, V]) Close() error {
	return nil
}
