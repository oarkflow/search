package storage

import (
	"container/list"

	"github.com/oarkflow/xsync"
)

type LRU[K comparable, V any] struct {
	capacity int
	cache    xsync.IMap[K, *list.Element]
	list     *list.List
}

type entry[K comparable, V any] struct {
	key   K
	value V
}

func NewLRU[K comparable, V any](capacity int) *LRU[K, V] {
	return &LRU[K, V]{
		capacity: capacity,
		cache:    xsync.NewMap[K, *list.Element](),
		list:     list.New(),
	}
}

func (l *LRU[K, V]) Get(key K) (V, bool) {
	if ele, ok := l.cache.Get(key); ok {
		l.list.MoveToFront(ele)
		return ele.Value.(*entry[K, V]).value, true
	}
	var zero V
	return zero, false
}

func (l *LRU[K, V]) Put(key K, value V) {
	if ele, ok := l.cache.Get(key); ok {
		l.list.MoveToFront(ele)
		ele.Value.(*entry[K, V]).value = value
		return
	}

	ele := l.list.PushFront(&entry[K, V]{key, value})
	l.cache.Set(key, ele)

	if l.list.Len() > l.capacity {
		l.removeOldest()
	}
}

func (l *LRU[K, V]) removeOldest() (K, V, bool) {
	var k K
	var v V
	ele := l.list.Back()
	if l.list == nil {
		l.list = list.New()
	}
	if ele != nil {
		l.list.Remove(ele)
		if ele.Value != nil {
			kv := ele.Value.(*entry[K, V])
			l.cache.Del(kv.key)
			return kv.key, kv.value, true
		}
	}
	return k, v, false
}

func (l *LRU[K, V]) Remove(key K) {
	if ele, ok := l.cache.Get(key); ok {
		l.list.Remove(ele)
		l.cache.Del(key)
	}
}
