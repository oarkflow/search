package janitor

import (
	"container/list"
	"log"
	"time"
)

// DataSource defines methods for getting and setting data.
type DataSource[K comparable, V any] interface {
	Get(key K) (V, bool)
	Set(key K, value V) error
	Del(key K) error
	SetEvictionHandler(func(key K, value V))
	EvictionHandler() func(key K, value V)
}

// Cleaner interface defines the cleanup method
type Cleaner interface {
	CleanUp()
	Stop()
}

// Janitor is a janitor that uses an LRU cache and cleans up at regular intervals
type Janitor[K comparable, V any] struct {
	source           DataSource[K, V]
	target           DataSource[K, V]
	cleanupFrequency time.Duration
	stopChan         chan struct{}
	targetAsFallback bool
}

// New creates a new Janitor with the given source, target, and cleanup frequency
func New[K comparable, V any](source, target DataSource[K, V], cleanupFrequency time.Duration, targetAsFallback bool) *Janitor[K, V] {
	janitor := &Janitor[K, V]{
		source:           source,
		target:           target,
		cleanupFrequency: cleanupFrequency,
		stopChan:         make(chan struct{}),
		targetAsFallback: targetAsFallback,
	}
	if source.EvictionHandler() == nil {
		source.SetEvictionHandler(janitor.HandleEviction)
	}
	return janitor
}

// HandleEviction handler for the cache
func (j *Janitor[K, V]) HandleEviction(key K, value V) {
	if j.target != nil {
		j.target.Set(key, value)
		log.Printf("Moved evicted key %v to target data source", key)
	} else {
		log.Printf("Evicted key %v from source without moving to target", key)
	}
}

// Get retrieves data from source or target if fallback is enabled
func (j *Janitor[K, V]) Get(key K) (V, bool) {
	// Try to get from source first
	if val, found := j.source.Get(key); found {
		return val, true
	}

	// If target is provided and targetAsFallback is true, try to get from target
	if j.targetAsFallback && j.target != nil {
		if val, found := j.target.Get(key); found {
			// Store it back in the source cache
			j.source.Set(key, val)
			return val, true
		}
	}

	var zeroValue V
	return zeroValue, false
}

// CleanUp runs the cleanup process periodically
func (j *Janitor[K, V]) CleanUp() {
	ticker := time.NewTicker(j.cleanupFrequency)
	for {
		select {
		case <-ticker.C:
			j.cleanupProcess()
		case <-j.stopChan:
			ticker.Stop()
			return
		}
	}
}

// cleanupProcess moves data from source to target if it's not accessed for a long time
func (j *Janitor[K, V]) cleanupProcess() {
	log.Println("Running cleanup process...")
	// Iterate over source cache and move least recently used items to target if target is provided
	caches := j.source.(*LRUCache[K, V])
	caches.data.ForEach(func(key K, _ *list.Element) bool {
		if _, found := j.source.Get(key); found {
			if j.target != nil {
				if value, exists := j.source.Get(key); exists {
					j.target.Set(key, value)
					j.source.Del(key)
					log.Printf("Moved key %v to target data source", key)
				}
			} else {
				j.source.Del(key)
				log.Printf("Removed key %v from source data source", key)
			}
		}
		return true
	})
}

// Stop stops the janitor's cleanup process
func (j *Janitor[K, V]) Stop() {
	close(j.stopChan)
}
