package mmap

import (
	"fmt"
	"log"
	"sort"
	"strings"

	"github.com/oarkflow/filters"
	"github.com/oarkflow/msgpack"
	"github.com/oarkflow/xsync"

	"github.com/oarkflow/search/storage"
	"github.com/oarkflow/search/storage/flydb"
)

// MapStats represents statistics of the map.
type MapStats struct {
	InMemory uint32
	OnDisk   int
}

// MMap is an implementation of IMap using xsync and pogreb.
type MMap[K comparable, V any] struct {
	inMemory    *xsync.MapOf[K, V]
	db          *flydb.FlyDB[string, []byte]
	comparator  storage.Comparator[K]
	path        string
	maxInMemory int
	sampleSize  int
	evictList   []K
}

func (m *MMap[K, V]) Len() uint32 {
	return uint32(m.inMemory.Size()) + m.db.Len()
}

func (m *MMap[K, V]) Name() string {
	return "memdb-persist"
}

func (m *MMap[K, V]) Sample(params storage.SampleParams) (map[string]V, error) {
	records, err := m.sampleFromMemory(params)
	if err != nil {
		return nil, err
	}
	if records == nil {
		records = make(map[string]V)
	}
	sz := m.sampleSize
	if params.Size != 0 {
		sz = params.Size
	}
	if len(records) >= sz {
		return records, nil
	}
	params.Size = sz - len(records)
	data, err := m.db.Sample(params)
	if err != nil {
		return nil, err
	}
	for key, val := range data {
		records[key] = decode[V](val)
	}
	return records, nil
}

func (m *MMap[K, V]) Close() error {
	m.inMemory.Clear()
	return m.db.Close()
}

// New creates a new MMap instance.
func New[K comparable, V any](dbPath string, maxInMemory, sampleSize int, comparator storage.Comparator[K]) (*MMap[K, V], error) {
	db, err := flydb.New[string, []byte](dbPath, maxInMemory)
	if err != nil {
		return nil, err
	}

	return &MMap[K, V]{
		inMemory:    xsync.NewMap[K, V](),
		db:          db,
		maxInMemory: maxInMemory,
		sampleSize:  sampleSize,
		comparator:  comparator,
		evictList:   make([]K, 0),
		path:        dbPath,
	}, nil
}

// Get retrieves a value from the map.
func (m *MMap[K, V]) Get(key K) (V, bool) {
	if value, ok := m.inMemory.Get(key); ok {
		return value, true
	}
	var value V
	// Load from disk if not found in memory
	raw, ok := m.db.Get(fmt.Sprintf("%v", key))
	if ok && raw != nil {
		value = decode[V](raw)
		m.inMemory.Set(key, value)
		m.evictList = append(m.evictList, key)
		if len(m.evictList) > m.maxInMemory {
			evictKey := m.evictList[0]
			m.evictList = m.evictList[1:]
			m.inMemory.Del(evictKey)
		}
		return value, true
	}

	var zeroValue V
	return zeroValue, false
}

func (m *MMap[K, V]) sampleFromMemory(params storage.SampleParams) (map[string]V, error) {
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
	m.inMemory.ForEach(func(key K, val V) bool {
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
		val, _ := m.inMemory.Get(key)
		tmp := fmt.Sprint(key)
		value[tmp] = val
		count++
	}

	return value, nil
}

// Set sets a value in the map.
func (m *MMap[K, V]) Set(key K, value V) error {
	m.inMemory.Set(key, value)
	m.evictList = append(m.evictList, key)
	if len(m.evictList) > m.maxInMemory {
		evictKey := m.evictList[0]
		m.evictList = m.evictList[1:]
		m.inMemory.Del(evictKey)
	}

	return m.db.Set(fmt.Sprintf("%v", key), encode(value))
}

// Del deletes the key from the map.
func (m *MMap[K, V]) Del(key K) error {
	m.inMemory.Del(key)
	return m.db.Del(fmt.Sprintf("%v", key))
}

// ForEach applies a function to each key-value pair in the map.
func (m *MMap[K, V]) ForEach(f func(K, V) bool) {
	m.inMemory.ForEach(func(key K, value V) bool {
		return f(key, value)
	})
}

// Clear clears all entries from the map.
func (m *MMap[K, V]) Clear() {
	m.inMemory = xsync.NewMap[K, V]()
	m.evictList = make([]K, 0)
	m.db = nil // Close and reopen db to clear it
	db, err := flydb.New[string, []byte](m.path, 100)
	if err != nil {
		log.Printf("Error reopening database: %v", err)
	}
	m.db = db
}

// encode and decode functions to handle type serialization.
func encode[V any](value V) []byte {
	jsonData, err := msgpack.Marshal(value)
	if err != nil {
		return nil
	}
	return jsonData
}

func decode[V any](data []byte) V {
	var value V
	err := msgpack.Unmarshal(data, &value)
	if err != nil {
		panic(err)
		return *new(V)
	}
	return value
}
