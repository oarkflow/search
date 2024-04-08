package storage

// Store defines the interface for our key-value store
type Store[K comparable, V any] interface {
	Set(key K, value V) error
	Get(key K) (V, bool)
	Del(key K) error
	Len() uint32
	Sample() (map[string]V, error)
	Close() error
}
