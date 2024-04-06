package search

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/shamaton/msgpack/v2"

	"github.com/oarkflow/flydb"
)

// KVStore defines the interface for our key-value store
type KVStore[K comparable, V any] interface {
	Set(key K, value V) error
	Get(key K) (V, bool)
	Del(key K) error
	Len() uint32
}

type FlyDB[K comparable, V any] struct {
	client   *flydb.DB[[]byte, []byte]
	compress bool
}

func NewFlyDB[K comparable, V any](basePath string, compressed bool) (*FlyDB[K, V], error) {
	client, err := flydb.Open[[]byte, []byte](basePath, nil)
	if err != nil {
		return nil, err
	}
	db := &FlyDB[K, V]{
		client:   client,
		compress: compressed,
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
		compressed, err := Compress(jsonData)
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
		jsonData, err := Decompress(compressedData)
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

// JsonStore implements KVStore using files on disk
type JsonStore[K comparable, V any] struct {
	basePath string
	compress bool
	docLen   int
}

// NewJsonStore creates a new JsonStore instance
func NewJsonStore[K comparable, V any](basePath string, compress bool) (KVStore[K, V], error) {
	if err := os.MkdirAll(basePath, 0755); err != nil {
		return nil, err
	}
	return &JsonStore[K, V]{basePath: basePath, compress: compress}, nil
}

// Set stores a key-value pair on disk
func (s *JsonStore[K, V]) Set(key K, value V) error {
	fileName := filepath.Join(s.basePath, fmt.Sprintf("%v.json", key))
	jsonData, err := msgpack.Marshal(value)
	if err != nil {
		return err
	}
	if s.compress {
		compressed, err := Compress(jsonData)
		if err != nil {
			return err
		}

		return os.WriteFile(fileName, compressed, 0644)
	}
	return os.WriteFile(fileName, jsonData, 0644)
}

// Close removes a key-value pair from disk
func (s *JsonStore[K, V]) Close() error {
	return nil
}

// Len removes a key-value pair from disk
func (s *JsonStore[K, V]) Len() uint32 {
	if s.docLen == 0 {
		dirs, err := os.ReadDir(s.basePath)
		if err != nil {
			return 0
		}
		total := 0
		for _, dir := range dirs {
			if !dir.IsDir() {
				total++
			}
		}
		s.docLen = total
	}

	return uint32(s.docLen)
}

// Get retrieves a value for a given key from disk
func (s *JsonStore[K, V]) Get(key K) (V, bool) {
	var err error
	fileName := filepath.Join(s.basePath, fmt.Sprintf("%v.json", key))
	_, err = os.Stat(fileName)
	if err != nil {
		if os.IsNotExist(err) {
			return *new(V), false
		}
		return *new(V), false
	}
	var value V
	if s.compress {
		compressedData, err := os.ReadFile(fileName)
		if err != nil {
			return *new(V), false
		}
		jsonData, err := Decompress(compressedData)
		err = msgpack.Unmarshal(jsonData, &value)
	} else {
		file, err := os.ReadFile(fileName)
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

// Del removes a key-value pair from disk
func (s *JsonStore[K, V]) Del(key K) error {
	fileName := filepath.Join(s.basePath, fmt.Sprintf("%v", key))
	return os.Remove(fileName)
}
