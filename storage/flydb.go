package storage

import (
	"fmt"

	"github.com/oarkflow/flydb"
	"github.com/oarkflow/msgpack"

	"github.com/oarkflow/search/lib"
)

type FlyDB[K comparable, V any] struct {
	client     *flydb.DB[[]byte, []byte]
	compress   bool
	sampleSize int
}

func NewFlyDB[K comparable, V any](basePath string, compressed bool, sampleSize int) (Store[K, V], error) {
	client, err := flydb.Open[[]byte, []byte](basePath, nil)
	if err != nil {
		return nil, err
	}
	db := &FlyDB[K, V]{
		client:     client,
		compress:   compressed,
		sampleSize: sampleSize,
	}
	return db, nil
}

func (s *FlyDB[K, V]) Set(key K, value V) error {
	k := fmt.Sprintf("%v", key)
	jsonData, err := msgpack.Marshal(value)
	if err != nil {
		return err
	}
	if s.compress {
		compressed, err := lib.Compress(jsonData)
		if err != nil {
			return err
		}
		return s.client.Put([]byte(k), compressed)
	}
	return s.client.Put([]byte(k), jsonData)
}

// Del removes a key-value pair from disk
func (s *FlyDB[K, V]) Del(key K) error {
	k := fmt.Sprintf("%v", key)
	return s.client.Delete([]byte(k))
}

// Sample removes a key-value pair from disk
func (s *FlyDB[K, V]) Sample(size ...int) (map[string]V, error) {
	sz := s.sampleSize
	if len(size) > 0 && size[0] != 0 {
		sz = size[0]
	}
	value := make(map[string]V)
	it := s.client.Items()
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
	var value V
	if s.compress {
		compressedData, err := s.client.Get([]byte(k))
		if err != nil {
			return *new(V), false
		}
		jsonData, err := lib.Decompress(compressedData)
		err = msgpack.Unmarshal(jsonData, &value)
	} else {
		file, err := s.client.Get([]byte(k))
		if err != nil {
			return *new(V), false
		}
		err = msgpack.Unmarshal(file, &value)
	}
	if err != nil {
		return *new(V), false
	}
	return value, true
}

func (s *FlyDB[K, V]) GetData(val []byte) (V, bool) {
	var value V
	var err error
	if s.compress {
		jsonData, err := lib.Decompress(val)
		if err != nil {
			return *new(V), false
		}
		err = msgpack.Unmarshal(jsonData, &value)
	} else {
		err = msgpack.Unmarshal(val, &value)
	}
	if err != nil {
		return *new(V), false
	}
	return value, true
}
