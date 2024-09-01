package janitor

import (
	"container/list"
	"errors"
	"sync"

	"github.com/oarkflow/xsync"
)

// LRUCache is a generic implementation of an LRU cache
type LRUCache[K comparable, V any] struct {
	capacity   int
	data       xsync.IMap[K, *list.Element]
	list       *list.List
	m          sync.Mutex
	onEviction func(K, V)
}

// Entry is a key-value pair stored in the cache
type Entry[K comparable, V any] struct {
	key   K
	value V
}

// NewLRUCache creates a new LRUCache with the given capacity
func NewLRUCache[K comparable, V any](capacity int) *LRUCache[K, V] {
	return &LRUCache[K, V]{
		capacity: capacity,
		data:     xsync.NewMap[K, *list.Element](),
		list:     list.New(),
	}
}

// Get retrieves a value from the cache and marks it as recently used
func (cache *LRUCache[K, V]) Get(key K) (V, bool) {
	cache.m.Lock()
	defer cache.m.Unlock()

	if elem, found := cache.data.Get(key); found {
		cache.list.MoveToFront(elem)
		return elem.Value.(*Entry[K, V]).value, true
	}
	var zeroValue V
	return zeroValue, false
}

func (cache *LRUCache[K, V]) SetEvictionHandler(onEviction func(K, V)) {
	cache.onEviction = onEviction
}

func (cache *LRUCache[K, V]) EvictionHandler() func(K, V) {
	return cache.onEviction
}

// Set adds a value to the cache, evicting the least recently used item if necessary
func (cache *LRUCache[K, V]) Set(key K, value V) error {
	cache.m.Lock()
	defer cache.m.Unlock()

	if elem, found := cache.data.Get(key); found {
		cache.list.MoveToFront(elem)
		elem.Value.(*Entry[K, V]).value = value
		return nil
	}

	if cache.list.Len() >= cache.capacity {
		oldest := cache.list.Back()
		if oldest != nil {
			cache.list.Remove(oldest)
			entry := oldest.Value.(*Entry[K, V])
			cache.data.Del(entry.key)

			// Call the eviction callback
			if cache.onEviction != nil {
				cache.onEviction(entry.key, entry.value)
			}
		}
	}

	entry := &Entry[K, V]{key, value}
	elem := cache.list.PushFront(entry)
	cache.data.Set(key, elem)
	return nil
}

// Del deletes a key from the cache
func (cache *LRUCache[K, V]) Del(key K) error {
	cache.m.Lock()
	defer cache.m.Unlock()
	elem, found := cache.data.Get(key)
	if !found {
		return errors.New("key not found")
	}
	cache.list.Remove(elem)
	cache.data.Del(key)
	return nil
}
