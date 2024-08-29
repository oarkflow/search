package flydb

import (
	"fmt"

	"github.com/oarkflow/filters"
	"github.com/oarkflow/flydb"
	"github.com/oarkflow/msgpack"
	"github.com/oarkflow/search/storage"
)

type FlyDB[K comparable, V any] struct {
	client     *flydb.DB[[]byte, []byte]
	sampleSize int
}

func New[K comparable, V any](basePath string, sampleSize int) (*FlyDB[K, V], error) {
	client, err := flydb.Open[[]byte, []byte](basePath, nil)
	if err != nil {
		return nil, err
	}
	db := &FlyDB[K, V]{
		client:     client,
		sampleSize: sampleSize,
	}
	return db, nil
}

func (s *FlyDB[K, V]) Name() string {
	return "flydb"
}

func (s *FlyDB[K, V]) Set(key K, value V) error {
	k := fmt.Sprintf("%v", key)
	jsonData, err := msgpack.Marshal(value)
	if err != nil {
		return err
	}
	return s.client.Put([]byte(k), jsonData)
}

// Del removes a key-value pair from disk
func (s *FlyDB[K, V]) Del(key K) error {
	k := fmt.Sprintf("%v", key)
	return s.client.Delete([]byte(k))
}

func (s *FlyDB[K, V]) ForEach(fn func(K, V) bool) {
	panic("implement me")
}

// Sample removes a key-value pair from disk
func (s *FlyDB[K, V]) Sample(params storage.SampleParams) (map[string]V, error) {
	sz := s.sampleSize
	if params.Size != 0 {
		sz = params.Size
	}
	value := make(map[string]V)
	it := s.client.Items()
	count := 0
	var conditions []filters.Condition
	for _, f := range params.Filters {
		conditions = append(conditions, f)
	}
	for count < sz {
		key, val, err := it.Next()
		if err == flydb.ErrIterationDone {
			break
		}
		data, exists := s.GetData(val)
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
	for i := 0; i < sz; i++ {
		key, val, err := it.Next()
		if err == flydb.ErrIterationDone {
			break
		}
		data, exists := s.GetData(val)
		if exists {
			value[string(key)] = data
		}
	}
	return value, nil
}

// Close removes a key-value pair from disk
func (s *FlyDB[K, V]) Close() error {
	return s.client.Close()
}

// Len removes a key-value pair from disk
func (s *FlyDB[K, V]) Len() uint32 {
	return s.client.Count()
}

// Get retrieves a value for a given key from disk
func (s *FlyDB[K, V]) Get(key K) (V, bool) {
	var err error
	k := fmt.Sprintf("%v", key)
	file, err := s.client.Get([]byte(k))
	if err != nil {
		return *new(V), false
	}
	return s.GetData(file)
}

func (s *FlyDB[K, V]) GetData(val []byte) (V, bool) {
	var value V
	err := msgpack.Unmarshal(val, &value)
	if err != nil {
		return *new(V), false
	}
	return value, true
}
