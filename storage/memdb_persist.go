package storage

import (
	"fmt"
	"sort"
	"strings"

	"github.com/oarkflow/filters"
	"github.com/oarkflow/flydb"
	"github.com/oarkflow/xsync"
	"github.com/vmihailenco/msgpack/v5"

	"github.com/oarkflow/search/lib"
)

type MapWithPersist[K comparable, V any] struct {
	m          xsync.IMap[K, V]
	lru        *LRU[K, V]
	maxMem     uint64
	docLen     *xsync.Counter
	db         *flydb.DB[[]byte, []byte]
	comparator Comparator[K]
	persist    bool
	sampleSize int
}

// NewMapWithPersist initializes a new MapWithPersist instance using MessagePack for serialization.
func NewMapWithPersist[K comparable, V any](maxMem uint64, sampleSize int, comparator Comparator[K], lruCapacity int, dbPath string, persist bool) (*MapWithPersist[K, V], error) {
	db, err := flydb.Open[[]byte, []byte](dbPath, nil)
	if err != nil {
		return nil, err
	}

	return &MapWithPersist[K, V]{
		m:          xsync.NewMap[K, V](),
		lru:        NewLRU[K, V](lruCapacity),
		maxMem:     maxMem,
		docLen:     xsync.NewCounter(),
		sampleSize: sampleSize,
		comparator: comparator,
		persist:    persist,
		db:         db,
	}, nil
}

func (mlru *MapWithPersist[K, V]) Name() string {
	return "persist"
}

func (mlru *MapWithPersist[K, V]) Sample(params SampleParams) (map[string]V, error) {
	sz := mlru.sampleSize
	if params.Size != 0 {
		sz = params.Size
	}
	value := make(map[string]V)
	it := mlru.db.Items()
	count := 0
	var conditions []filters.Condition
	for _, f := range params.Filters {
		conditions = append(conditions, f)
	}
	var keys []K
	if mlru.m.Size() > 0 && mlru.m.Size() >= mlru.sampleSize {
		mlru.m.ForEach(func(key K, val V) bool {
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
				return mlru.comparator(keys[i], keys[j]) < 0
			})
		} else if strings.ToLower(params.Sort) == "desc" {
			sort.Slice(keys, func(i, j int) bool {
				return mlru.comparator(keys[i], keys[j]) > 0
			})
		}
		for _, key := range keys {
			if count >= sz {
				break
			}
			val, _ := mlru.m.Get(key)
			tmp := fmt.Sprint(key)
			value[tmp] = val
			count++
		}
		return value, nil
	}
	if !mlru.persist {
		return nil, nil
	}
	for count < sz {
		key, val, err := it.Next()
		if err == flydb.ErrIterationDone {
			break
		}
		data, exists := mlru.GetData(val)
		if exists {
			if params.Rule != nil {
				if params.Rule.Match(data) {
					tmp := fmt.Sprint(key)
					value[tmp] = data
					count++
				}
			} else if params.Filters != nil {
				if filters.MatchGroup(val, &filters.FilterGroup{Operator: filters.AND, Filters: conditions}) {
					tmp := fmt.Sprint(key)
					value[tmp] = data
					count++
				}
			} else {
				value[string(key)] = data
				count++
			}
		}
	}
	return value, nil
}

func (mlru *MapWithPersist[K, V]) Set(key K, value V) error {
	mlru.m.Set(key, value)
	mlru.lru.Put(key, value)
	mlru.docLen.Inc()
	if mlru.persist {
		go mlru.checkMemoryAndPersist()
	}
	return nil
}

func (mlru *MapWithPersist[K, V]) Del(key K) error {
	mlru.m.Del(key)
	mlru.lru.Remove(key)
	mlru.docLen.Dec()
	return nil
}

func (mlru *MapWithPersist[K, V]) Len() uint32 {
	return uint32(mlru.docLen.Value())
}

func (mlru *MapWithPersist[K, V]) Get(key K) (V, bool) {
	var zero V
	if value, ok := mlru.lru.Get(key); ok {
		return value, true
	}
	if value, ok := mlru.m.Get(key); ok {
		mlru.lru.Put(key, value)
		return value, true
	}
	if !mlru.persist {
		return zero, false
	}
	if value, ok := mlru.restoreFromStore(key); ok {
		mlru.Set(key, value)
		return value, true
	}

	return zero, false
}

func (mlru *MapWithPersist[K, V]) checkMemoryAndPersist() {
	if lib.GetMemoryUsage() > mlru.maxMem {
		mlru.persistLeastUsed()
	}
}

func (mlru *MapWithPersist[K, V]) persistLeastUsed() {
	key, value, ok := mlru.lru.removeOldest()
	if !ok {
		return
	}
	keyBytes, err := lib.Serialize(key)
	if err != nil {
		return
	}
	valueBytes, err := lib.Serialize(value)
	if err != nil {
		return
	}
	if err := mlru.db.Put(keyBytes, valueBytes); err != nil {
		return
	}
	mlru.m.Del(key)
}

func (mlru *MapWithPersist[K, V]) restoreFromStore(key K) (V, bool) {
	keyBytes, err := lib.Serialize(key)
	if err != nil {
		return *new(V), false
	}
	valueBytes, err := mlru.db.Get(keyBytes)
	if err != nil || valueBytes == nil {
		return *new(V), false
	}
	value, err := lib.Deserialize[V](valueBytes)
	if err != nil {
		return *new(V), false
	}

	return value, true
}

func (mlru *MapWithPersist[K, V]) Close() error {
	return mlru.db.Close()
}

func (mlru *MapWithPersist[K, V]) GetData(val []byte) (V, bool) {
	var value V
	err := msgpack.Unmarshal(val, &value)
	if err != nil {
		return *new(V), false
	}
	return value, true
}
