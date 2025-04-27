package main

//
//import (
//	"container/heap"
//	"context"
//	"encoding/json"
//	"fmt"
//	"log"
//	"os"
//	"path/filepath"
//	"sort"
//	"sync"
//	"time"
//)
//
//type Comparator[K any] func(a, b K) int
//
//func findIndex[K any](keys []K, key K, comp Comparator[K]) int {
//	return sort.Search(len(keys), func(i int) bool {
//		return comp(keys[i], key) >= 0
//	})
//}
//
//const treeOrder = 32
//
//type BPTreeNode[K any, V any] struct {
//	isLeaf bool
//	keys   []K
//
//	values   []V
//	children []*BPTreeNode[K, V]
//
//	next *BPTreeNode[K, V]
//}
//
//type BPTree[K any, V any] struct {
//	root *BPTreeNode[K, V]
//	comp Comparator[K]
//	mu   sync.RWMutex
//}
//
//func NewBPTree[K any, V any](comp Comparator[K]) *BPTree[K, V] {
//	return &BPTree[K, V]{root: &BPTreeNode[K, V]{isLeaf: true}, comp: comp}
//}
//
//func (t *BPTree[K, V]) Search(key K) V {
//	t.mu.RLock()
//	defer t.mu.RUnlock()
//	node := t.root
//	for {
//		if node.isLeaf {
//			i := findIndex(node.keys, key, t.comp)
//			if i < len(node.keys) && t.comp(node.keys[i], key) == 0 {
//				return node.values[i]
//			}
//			var zero V
//			return zero
//		}
//		i := sort.Search(len(node.keys), func(i int) bool {
//			return t.comp(key, node.keys[i]) < 0
//		})
//		node = node.children[i]
//	}
//}
//
//func (t *BPTree[K, V]) Insert(key K, value V) {
//	t.mu.Lock()
//	defer t.mu.Unlock()
//	newKey, newNode := t.insert(t.root, key, value)
//	if newNode != nil {
//
//		t.root = &BPTreeNode[K, V]{
//			isLeaf:   false,
//			keys:     []K{newKey},
//			children: []*BPTreeNode[K, V]{t.root, newNode},
//		}
//	}
//}
//
//func (t *BPTree[K, V]) insert(node *BPTreeNode[K, V], key K, value V) (K, *BPTreeNode[K, V]) {
//	if node.isLeaf {
//		i := findIndex(node.keys, key, t.comp)
//		if i < len(node.keys) && t.comp(node.keys[i], key) == 0 {
//
//			node.values[i] = value
//			var zero K
//			return zero, nil
//		}
//
//		node.keys = append(node.keys, key)
//		copy(node.keys[i+1:], node.keys[i:])
//		node.keys[i] = key
//
//		node.values = append(node.values, value)
//		copy(node.values[i+1:], node.values[i:])
//		node.values[i] = value
//
//		if len(node.keys) < treeOrder {
//			var zero K
//			return zero, nil
//		}
//		return t.splitLeaf(node)
//	}
//
//	i := sort.Search(len(node.keys), func(i int) bool {
//		return t.comp(key, node.keys[i]) < 0
//	})
//	newKey, newChild := t.insert(node.children[i], key, value)
//	if newChild == nil {
//		var zero K
//		return zero, nil
//	}
//
//	node.keys = append(node.keys, newKey)
//	copy(node.keys[i+1:], node.keys[i:])
//	node.keys[i] = newKey
//
//	node.children = append(node.children, nil)
//	copy(node.children[i+2:], node.children[i+1:])
//	node.children[i+1] = newChild
//
//	if len(node.keys) < treeOrder {
//		var zero K
//		return zero, nil
//	}
//	return t.splitInternal(node)
//}
//
//func (t *BPTree[K, V]) splitLeaf(node *BPTreeNode[K, V]) (K, *BPTreeNode[K, V]) {
//	splitIndex := len(node.keys) / 2
//	newNode := &BPTreeNode[K, V]{
//		isLeaf: true,
//		keys:   append([]K(nil), node.keys[splitIndex:]...),
//		values: append([]V(nil), node.values[splitIndex:]...),
//		next:   node.next,
//	}
//	node.keys = node.keys[:splitIndex]
//	node.values = node.values[:splitIndex]
//	node.next = newNode
//	return newNode.keys[0], newNode
//}
//
//func (t *BPTree[K, V]) splitInternal(node *BPTreeNode[K, V]) (K, *BPTreeNode[K, V]) {
//	splitIndex := len(node.keys) / 2
//	promoted := node.keys[splitIndex]
//	newNode := &BPTreeNode[K, V]{
//		isLeaf:   false,
//		keys:     append([]K(nil), node.keys[splitIndex+1:]...),
//		children: append([]*BPTreeNode[K, V](nil), node.children[splitIndex+1:]...),
//	}
//	node.keys = node.keys[:splitIndex]
//	node.children = node.children[:splitIndex+1]
//	return promoted, newNode
//}
//
//func (t *BPTree[K, V]) Delete(key K) {
//	t.mu.Lock()
//	defer t.mu.Unlock()
//	t.deleteRecursive(nil, t.root, key, -1)
//
//	if !t.root.isLeaf && len(t.root.children) == 1 {
//		t.root = t.root.children[0]
//	}
//}
//
//func (t *BPTree[K, V]) deleteRecursive(parent *BPTreeNode[K, V], node *BPTreeNode[K, V], key K, childIndex int) bool {
//
//	minKeysLeaf := (treeOrder + 1) / 2
//	minKeysInternal := (treeOrder+1)/2 - 1
//	if node.isLeaf {
//		i := findIndex(node.keys, key, t.comp)
//		if i >= len(node.keys) || t.comp(node.keys[i], key) != 0 {
//
//			return false
//		}
//
//		node.keys = append(node.keys[:i], node.keys[i+1:]...)
//		node.values = append(node.values[:i], node.values[i+1:]...)
//		if parent == nil {
//			return false
//		}
//		if len(node.keys) >= minKeysLeaf {
//			return false
//		}
//		return t.rebalanceLeaf(parent, node, childIndex, minKeysLeaf)
//	}
//
//	i := 0
//	for i < len(node.keys) && t.comp(key, node.keys[i]) >= 0 {
//		i++
//	}
//	underflow := t.deleteRecursive(node, node.children[i], key, i)
//	if !underflow {
//		return false
//	}
//	return t.rebalanceInternal(node, i, minKeysInternal)
//}
//
//func (t *BPTree[K, V]) rebalanceLeaf(parent, leaf *BPTreeNode[K, V], idx, minLeaf int) bool {
//
//	if idx > 0 {
//		leftSibling := parent.children[idx-1]
//		if len(leftSibling.keys) > minLeaf {
//
//			borrowKey := leftSibling.keys[len(leftSibling.keys)-1]
//			borrowValue := leftSibling.values[len(leftSibling.values)-1]
//			leftSibling.keys = leftSibling.keys[:len(leftSibling.keys)-1]
//			leftSibling.values = leftSibling.values[:len(leftSibling.values)-1]
//			leaf.keys = append([]K{borrowKey}, leaf.keys...)
//			leaf.values = append([]V{borrowValue}, leaf.values...)
//
//			parent.keys[idx-1] = leaf.keys[0]
//			return false
//		}
//	}
//
//	if idx < len(parent.children)-1 {
//		rightSibling := parent.children[idx+1]
//		if len(rightSibling.keys) > minLeaf {
//			borrowKey := rightSibling.keys[0]
//			borrowValue := rightSibling.values[0]
//			rightSibling.keys = rightSibling.keys[1:]
//			rightSibling.values = rightSibling.values[1:]
//			leaf.keys = append(leaf.keys, borrowKey)
//			leaf.values = append(leaf.values, borrowValue)
//			parent.keys[idx] = rightSibling.keys[0]
//			return false
//		}
//	}
//
//	if idx > 0 {
//		leftSibling := parent.children[idx-1]
//		leftSibling.keys = append(leftSibling.keys, leaf.keys...)
//		leftSibling.values = append(leftSibling.values, leaf.values...)
//		leftSibling.next = leaf.next
//
//		parent.children = append(parent.children[:idx], parent.children[idx+1:]...)
//		parent.keys = append(parent.keys[:idx-1], parent.keys[idx:]...)
//	} else {
//		rightSibling := parent.children[idx+1]
//		leaf.keys = append(leaf.keys, rightSibling.keys...)
//		leaf.values = append(leaf.values, rightSibling.values...)
//		leaf.next = rightSibling.next
//		parent.children = append(parent.children[:idx+1], parent.children[idx+2:]...)
//		parent.keys = append(parent.keys[:idx], parent.keys[idx+1:]...)
//	}
//	if parent == nil {
//		return false
//	}
//	return len(parent.keys) < ((treeOrder+1)/2 - 1)
//}
//
//func (t *BPTree[K, V]) rebalanceInternal(parent *BPTreeNode[K, V], idx, minInternal int) bool {
//	child := parent.children[idx]
//	if idx > 0 {
//		leftSibling := parent.children[idx-1]
//		if len(leftSibling.keys) > minInternal {
//			separator := parent.keys[idx-1]
//			borrowKey := leftSibling.keys[len(leftSibling.keys)-1]
//			borrowChild := leftSibling.children[len(leftSibling.children)-1]
//			leftSibling.keys = leftSibling.keys[:len(leftSibling.keys)-1]
//			leftSibling.children = leftSibling.children[:len(leftSibling.children)-1]
//			child.keys = append([]K{separator}, child.keys...)
//			child.children = append([]*BPTreeNode[K, V]{borrowChild}, child.children...)
//			parent.keys[idx-1] = borrowKey
//			return false
//		}
//	}
//	if idx < len(parent.children)-1 {
//		rightSibling := parent.children[idx+1]
//		if len(rightSibling.keys) > minInternal {
//			separator := parent.keys[idx]
//			borrowKey := rightSibling.keys[0]
//			borrowChild := rightSibling.children[0]
//			rightSibling.keys = rightSibling.keys[1:]
//			rightSibling.children = rightSibling.children[1:]
//			child.keys = append(child.keys, separator)
//			child.children = append(child.children, borrowChild)
//			parent.keys[idx] = borrowKey
//			return false
//		}
//	}
//
//	if idx > 0 {
//		leftSibling := parent.children[idx-1]
//		separator := parent.keys[idx-1]
//		leftSibling.keys = append(leftSibling.keys, separator)
//		leftSibling.keys = append(leftSibling.keys, child.keys...)
//		leftSibling.children = append(leftSibling.children, child.children...)
//		parent.children = append(parent.children[:idx], parent.children[idx+1:]...)
//		parent.keys = append(parent.keys[:idx-1], parent.keys[idx:]...)
//	} else {
//		rightSibling := parent.children[idx+1]
//		separator := parent.keys[idx]
//		child.keys = append(child.keys, separator)
//		child.keys = append(child.keys, rightSibling.keys...)
//		child.children = append(child.children, rightSibling.children...)
//		parent.children = append(parent.children[:idx+1], parent.children[idx+2:]...)
//		parent.keys = append(parent.keys[:idx], parent.keys[idx+1:]...)
//	}
//	if parent == nil {
//		return false
//	}
//	return len(parent.keys) < ((treeOrder+1)/2 - 1)
//}
//
//type CacheItem[K any, V any] struct {
//	key        K
//	value      V
//	dirty      bool
//	lastAccess time.Time
//	index      int
//}
//
//type AccessHeap[K any, V any] []*CacheItem[K, V]
//
//func (h AccessHeap[K, V]) Len() int { return len(h) }
//func (h AccessHeap[K, V]) Less(i, j int) bool {
//	return h[i].lastAccess.Before(h[j].lastAccess)
//}
//func (h AccessHeap[K, V]) Swap(i, j int) {
//	h[i], h[j] = h[j], h[i]
//	h[i].index = i
//	h[j].index = j
//}
//func (h *AccessHeap[K, V]) Push(x any) {
//	item := x.(*CacheItem[K, V])
//	item.index = len(*h)
//	*h = append(*h, item)
//}
//func (h *AccessHeap[K, V]) Pop() any {
//	old := *h
//	n := len(old)
//	item := old[n-1]
//	item.index = -1
//	*h = old[0 : n-1]
//	return item
//}
//func (h *AccessHeap[K, V]) update(item *CacheItem[K, V], lastAccess time.Time) {
//	item.lastAccess = lastAccess
//	heap.Fix(h, item.index)
//}
//
//type PersistentLRU[K any, V any] struct {
//	capacity int
//	storeDir string
//	mu       sync.Mutex
//	tree     *BPTree[K, *CacheItem[K, V]]
//	heap     AccessHeap[K, V]
//	size     int
//	comp     Comparator[K]
//}
//
//func NewPersistentLRU[K any, V any](capacity int, storeDir string, comp Comparator[K]) (*PersistentLRU[K, V], error) {
//	if err := os.MkdirAll(storeDir, os.ModePerm); err != nil {
//		return nil, fmt.Errorf("cannot create storage directory: %w", err)
//	}
//	return &PersistentLRU[K, V]{
//		capacity: capacity,
//		storeDir: storeDir,
//		tree:     NewBPTree[K, *CacheItem[K, V]](comp),
//		heap:     make(AccessHeap[K, V], 0, capacity),
//		comp:     comp,
//	}, nil
//}
//
//func (lru *PersistentLRU[K, V]) fileName(key K) string {
//
//	return filepath.Join(lru.storeDir, fmt.Sprintf("doc_%v.json", key))
//}
//
//func (lru *PersistentLRU[K, V]) persistToDisk(key K, value V) error {
//	data, err := json.Marshal(value)
//	if err != nil {
//		return fmt.Errorf("marshal error: %w", err)
//	}
//	filename := lru.fileName(key)
//	if err := os.WriteFile(filename, data, 0644); err != nil {
//		return fmt.Errorf("write error: %w", err)
//	}
//	return nil
//}
//
//func (lru *PersistentLRU[K, V]) loadFromDisk(key K) (V, error) {
//	data, err := os.ReadFile(lru.fileName(key))
//	if err != nil {
//		var zero V
//		return zero, fmt.Errorf("read error: %w", err)
//	}
//	var value V
//	if err := json.Unmarshal(data, &value); err != nil {
//		var zero V
//		return zero, fmt.Errorf("unmarshal error: %w", err)
//	}
//	return value, nil
//}
//
//func (lru *PersistentLRU[K, V]) updateItemUsage(item *CacheItem[K, V]) {
//	now := time.Now()
//	item.lastAccess = now
//	heap.Fix(&lru.heap, item.index)
//}
//
//func (lru *PersistentLRU[K, V]) evictIfNeeded() {
//	for lru.size > lru.capacity {
//		item := heap.Pop(&lru.heap).(*CacheItem[K, V])
//		lru.tree.Delete(item.key)
//		lru.size--
//		if item.dirty {
//			if err := lru.persistToDisk(item.key, item.value); err != nil {
//				log.Printf("flush eviction key %v: %v", item.key, err)
//			}
//		}
//		log.Printf("Evicted key %v from cache", item.key)
//	}
//}
//
//func (lru *PersistentLRU[K, V]) Get(key K) (V, error) {
//	lru.mu.Lock()
//	defer lru.mu.Unlock()
//	item := lru.tree.Search(key)
//	if item != nil {
//		lru.updateItemUsage(item)
//		return item.value, nil
//	}
//
//	value, err := lru.loadFromDisk(key)
//	if err != nil {
//		var zero V
//		return zero, err
//	}
//	newItem := &CacheItem[K, V]{
//		key:        key,
//		value:      value,
//		dirty:      false,
//		lastAccess: time.Now(),
//	}
//	lru.tree.Insert(key, newItem)
//	heap.Push(&lru.heap, newItem)
//	lru.size++
//	lru.evictIfNeeded()
//	return value, nil
//}
//
//func (lru *PersistentLRU[K, V]) Set(key K, value V) error {
//	lru.mu.Lock()
//	defer lru.mu.Unlock()
//	now := time.Now()
//	item := lru.tree.Search(key)
//	if item != nil {
//		item.value = value
//		item.dirty = true
//		item.lastAccess = now
//		heap.Fix(&lru.heap, item.index)
//	} else {
//		newItem := &CacheItem[K, V]{
//			key:        key,
//			value:      value,
//			dirty:      true,
//			lastAccess: now,
//		}
//		lru.tree.Insert(key, newItem)
//		heap.Push(&lru.heap, newItem)
//		lru.size++
//		lru.evictIfNeeded()
//	}
//	return lru.persistToDisk(key, value)
//}
//
//func (lru *PersistentLRU[K, V]) Delete(key K) error {
//	lru.mu.Lock()
//	defer lru.mu.Unlock()
//	lru.tree.Delete(key)
//
//	for i, it := range lru.heap {
//		if lru.comp(it.key, key) == 0 {
//			heap.Remove(&lru.heap, i)
//			break
//		}
//	}
//	lru.size--
//	if err := os.Remove(lru.fileName(key)); err != nil && !os.IsNotExist(err) {
//		return fmt.Errorf("delete file: %w", err)
//	}
//	return nil
//}
//
//func (lru *PersistentLRU[K, V]) flushDirty() {
//	lru.mu.Lock()
//	defer lru.mu.Unlock()
//	for _, item := range lru.heap {
//		if item.dirty {
//			if err := lru.persistToDisk(item.key, item.value); err != nil {
//				log.Printf("flush error key %v: %v", item.key, err)
//			} else {
//				item.dirty = false
//			}
//		}
//	}
//}
//
//func (lru *PersistentLRU[K, V]) logUsage() {
//	lru.mu.Lock()
//	defer lru.mu.Unlock()
//	log.Printf("[Cache] %d items in memory (capacity: %d)", lru.size, lru.capacity)
//}
//
//func (lru *PersistentLRU[K, V]) StartBackgroundFlush(ctx context.Context, interval time.Duration) {
//	ticker := time.NewTicker(interval)
//	go func() {
//		defer ticker.Stop()
//		for {
//			select {
//			case <-ticker.C:
//				lru.flushDirty()
//				lru.logUsage()
//			case <-ctx.Done():
//				return
//			}
//		}
//	}()
//}
//
//func StoreJSONRecords[K any](records []map[string]any, keyExtractor func(map[string]any) K, cache *PersistentLRU[K, map[string]any]) error {
//	for _, rec := range records {
//		key := keyExtractor(rec)
//		if err := cache.Set(key, rec); err != nil {
//			return fmt.Errorf("storing record with key %v: %w", key, err)
//		}
//	}
//	return nil
//}
//
//func intComparator(a, b int) int {
//	return a - b
//}
//
//func stringComparator(a, b string) int {
//	if a < b {
//		return -1
//	} else if a > b {
//		return 1
//	}
//	return 0
//}
//
//func main() {
//	storeDir := "./storage"
//	cache, err := NewPersistentLRU[int, map[string]any](3, storeDir, intComparator)
//	if err != nil {
//		log.Fatalf("Error initializing PersistentLRU: %v", err)
//	}
//	ctx, cancel := context.WithCancel(context.Background())
//	defer cancel()
//	cache.StartBackgroundFlush(ctx, 10*time.Second)
//	for i := 1; i <= 5; i++ {
//		rec := map[string]any{
//			"id":      i,
//			"content": fmt.Sprintf("This is document number %d", i),
//			"meta":    fmt.Sprintf("metadata-%d", i),
//		}
//		if err := cache.Set(i, rec); err != nil {
//			log.Printf("Error setting key %d: %v", i, err)
//		} else {
//			log.Printf("Inserted key %d", i)
//		}
//		time.Sleep(500 * time.Millisecond)
//	}
//	for i := 3; i <= 5; i++ {
//		if rec, err := cache.Get(i); err != nil {
//			log.Printf("Error getting key %d: %v", i, err)
//		} else {
//			log.Printf("Accessed key %d: %v", i, rec)
//		}
//	}
//	jsonRecords := []map[string]any{
//		{"id": 10, "content": "Record 10", "meta": "bulk-10"},
//		{"id": 11, "content": "Record 11", "meta": "bulk-11"},
//		{"id": 12, "content": "Record 12", "meta": "bulk-12"},
//	}
//	err = StoreJSONRecords[int](jsonRecords, func(rec map[string]any) int {
//		switch v := rec["id"].(type) {
//		case float64:
//			return int(v)
//		case int:
//			return v
//		default:
//			return 0
//		}
//	}, cache)
//	if err != nil {
//		log.Printf("Error storing JSON records: %v", err)
//	} else {
//		log.Printf("Stored JSON records in bulk")
//	}
//	cache.logUsage()
//	log.Println("Demo complete.")
//	time.Sleep(1 * time.Minute)
//}
