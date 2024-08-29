package mmap

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/oarkflow/search/lib"
	"github.com/oarkflow/search/storage"
	"github.com/oarkflow/search/storage/flydb"
	"github.com/oarkflow/search/storage/memdb"
)

type MMap[K storage.Hashable, V any] struct {
	inMemory      *memdb.MemDB[K, V]
	db            *flydb.FlyDB[string, []byte]
	comparator    storage.Comparator[K]
	path          string
	maxInMemory   int
	sampleSize    int
	evictList     []K
	lastAccessed  map[K]time.Time
	mu            sync.Mutex
	cleanupPeriod time.Duration
}

func (m *MMap[K, V]) janitor(evictionDuration time.Duration) {
	ticker := time.NewTicker(m.cleanupPeriod)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			now := time.Now()
			m.mu.Lock()
			log.Println("Performing cleanup...")
			for key, lastAccess := range m.lastAccessed {
				if now.Sub(lastAccess) > evictionDuration {
					m.inMemory.Del(key)
					delete(m.lastAccessed, key)
					for i, evictKey := range m.evictList {
						if evictKey == key {
							m.evictList = append(m.evictList[:i], m.evictList[i+1:]...)
							break
						}
					}
				}
			}
			m.mu.Unlock()
		}
	}
}

func (m *MMap[K, V]) Len() uint32 {
	return m.inMemory.Len() + m.db.Len()
}

func (m *MMap[K, V]) Name() string {
	return "mmap"
}

func (m *MMap[K, V]) Sample(params storage.SampleParams) (map[string]V, error) {
	records, err := m.inMemory.Sample(params)
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
		records[key] = lib.Decode[V](val)
	}
	return records, nil
}

func (m *MMap[K, V]) Close() error {
	m.inMemory.Close()
	return m.db.Close()
}

type Config struct {
	Path               string        `json:"path"`
	MaxRecordsInMemory int           `json:"max_in_memory"`
	SampleSize         int           `json:"sample_size"`
	CleanupPeriod      time.Duration `json:"cleanup_period"`
	EvictionDuration   time.Duration `json:"eviction_duration"`
}

func New[K storage.Hashable, V any](cfg Config, comparator storage.Comparator[K]) (*MMap[K, V], error) {
	if cfg.MaxRecordsInMemory == 0 {
		cfg.MaxRecordsInMemory = 500
	}
	if cfg.SampleSize == 0 {
		cfg.SampleSize = 100
	}
	db, err := flydb.New[string, []byte](cfg.Path, cfg.MaxRecordsInMemory)
	if err != nil {
		return nil, err
	}
	store, err := memdb.New[K, V](cfg.SampleSize, comparator)
	if err != nil {
		return nil, err
	}
	if cfg.CleanupPeriod == 0 {
		cfg.CleanupPeriod = 5 * time.Minute
	}
	if cfg.EvictionDuration == 0 {
		cfg.EvictionDuration = 30 * time.Minute
	}
	mmap := &MMap[K, V]{
		inMemory:      store,
		db:            db,
		maxInMemory:   cfg.MaxRecordsInMemory,
		sampleSize:    cfg.SampleSize,
		comparator:    comparator,
		evictList:     make([]K, 0),
		path:          cfg.Path,
		lastAccessed:  make(map[K]time.Time),
		cleanupPeriod: cfg.CleanupPeriod,
	}
	go mmap.janitor(cfg.EvictionDuration)
	return mmap, nil
}

func (m *MMap[K, V]) Get(key K) (V, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if value, ok := m.inMemory.Get(key); ok {
		m.lastAccessed[key] = time.Now()
		return value, true
	}

	var value V
	raw, ok := m.db.Get(fmt.Sprintf("%v", key))
	if ok && raw != nil {
		value = lib.Decode[V](raw)
		m.inMemory.Set(key, value)
		m.lastAccessed[key] = time.Now()
		m.evictList = append(m.evictList, key)
		if len(m.evictList) > m.maxInMemory {
			evictKey := m.evictList[0]
			m.evictList = m.evictList[1:]
			m.inMemory.Del(evictKey)
			delete(m.lastAccessed, evictKey)
		}
		return value, true
	}

	var zeroValue V
	return zeroValue, false
}

func (m *MMap[K, V]) Set(key K, value V) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.inMemory.Set(key, value)
	m.lastAccessed[key] = time.Now()
	m.evictList = append(m.evictList, key)
	if len(m.evictList) > m.maxInMemory {
		evictKey := m.evictList[0]
		m.evictList = m.evictList[1:]
		m.inMemory.Del(evictKey)
		delete(m.lastAccessed, evictKey)
	}

	return m.db.Set(fmt.Sprintf("%v", key), lib.Encode(value))
}

func (m *MMap[K, V]) Del(key K) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.inMemory.Del(key)
	delete(m.lastAccessed, key)

	return m.db.Del(fmt.Sprintf("%v", key))
}

func (m *MMap[K, V]) Clear() {
	m.mu.Lock()
	defer m.mu.Unlock()

	store, err := memdb.New[K, V](m.sampleSize, m.comparator)
	if err != nil {
		return
	}
	m.inMemory = store
	m.evictList = make([]K, 0)
	m.lastAccessed = make(map[K]time.Time)
	m.db = nil // Close and reopen db to clear it
	db, err := flydb.New[string, []byte](m.path, 100)
	if err != nil {
		log.Printf("Error reopening database: %v", err)
	}
	m.db = db
}
