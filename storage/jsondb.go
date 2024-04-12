package storage

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/vmihailenco/msgpack/v5"

	"github.com/oarkflow/search/lib"
)

// JsonDB implements KVStore using files on disk
type JsonDB[K comparable, V any] struct {
	basePath string
	compress bool
	docLen   int
}

// NewJsonDB creates a new JsonDB instance
func NewJsonDB[K comparable, V any](basePath string, compress bool) (Store[K, V], error) {
	if err := os.MkdirAll(basePath, 0755); err != nil {
		return nil, err
	}
	return &JsonDB[K, V]{basePath: basePath, compress: compress}, nil
}

// Sample removes a key-value pair from disk
func (s *JsonDB[K, V]) Sample() (map[string]V, error) {
	return nil, nil
}

// Set stores a key-value pair on disk
func (s *JsonDB[K, V]) Set(key K, value V) error {
	fileName := filepath.Join(s.basePath, fmt.Sprintf("%v.json", key))
	jsonData, err := msgpack.Marshal(value)
	if err != nil {
		return err
	}
	if s.compress {
		compressed, err := lib.Compress(jsonData)
		if err != nil {
			return err
		}

		return os.WriteFile(fileName, compressed, 0644)
	}
	return os.WriteFile(fileName, jsonData, 0644)
}

// Close removes a key-value pair from disk
func (s *JsonDB[K, V]) Close() error {
	return nil
}

// Len removes a key-value pair from disk
func (s *JsonDB[K, V]) Len() uint32 {
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
func (s *JsonDB[K, V]) Get(key K) (V, bool) {
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
		jsonData, err := lib.Decompress(compressedData)
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
func (s *JsonDB[K, V]) Del(key K) error {
	fileName := filepath.Join(s.basePath, fmt.Sprintf("%v", key))
	return os.Remove(fileName)
}
