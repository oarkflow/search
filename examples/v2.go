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
//	"strconv"
//	"sync"
//	"time"
//)
//
//type GenericRecord map[string]any
//
//func (rec GenericRecord) String() string {
//	var keys []string
//	for k := range rec {
//		keys = append(keys, k)
//	}
//	sort.Strings(keys)
//	var parts []string
//	for _, k := range keys {
//		switch v := rec[k].(type) {
//		case string:
//			parts = append(parts, v)
//		case json.Number:
//			if i, err := v.Int64(); err == nil {
//				parts = append(parts, strconv.FormatInt(i, 10))
//			} else if f, err := v.Float64(); err == nil {
//				parts = append(parts, strconv.FormatFloat(f, 'f', -1, 64))
//			}
//		case float64:
//			parts = append(parts, strconv.FormatFloat(v, 'f', -1, 64))
//		case int:
//			parts = append(parts, strconv.Itoa(v))
//		default:
//		}
//	}
//	return joinStrings(parts, " ")
//}
//
//func joinStrings(ss []string, sep string) string {
//	out := ""
//	for i, s := range ss {
//		if i > 0 {
//			out += sep
//		}
//		out += s
//	}
//	return out
//}
//
//const treeOrder = 32
//
//type BPTreeNode struct {
//	isLeaf   bool
//	keys     []int
//	values   []*CacheItem
//	children []*BPTreeNode
//	next     *BPTreeNode
//}
//
//type BPTree struct {
//	root *BPTreeNode
//	mu   sync.RWMutex
//}
//
//func NewBPTree() *BPTree {
//	return &BPTree{root: &BPTreeNode{isLeaf: true}}
//}
//
//func (t *BPTree) Search(key int) *CacheItem {
//	t.mu.RLock()
//	defer t.mu.RUnlock()
//	node := t.root
//	for {
//		if node.isLeaf {
//			i := sort.SearchInts(node.keys, key)
//			if i < len(node.keys) && node.keys[i] == key {
//				return node.values[i]
//			}
//			return nil
//		}
//		i := sort.Search(len(node.keys), func(i int) bool {
//			return key < node.keys[i]
//		})
//		node = node.children[i]
//	}
//}
//
//func (t *BPTree) Insert(key int, item *CacheItem) {
//	t.mu.Lock()
//	defer t.mu.Unlock()
//	newKey, newNode := t.insert(t.root, key, item)
//	if newNode != nil {
//		newRoot := &BPTreeNode{
//			isLeaf:   false,
//			keys:     []int{newKey},
//			children: []*BPTreeNode{t.root, newNode},
//		}
//		t.root = newRoot
//	}
//}
//
//func (t *BPTree) insert(node *BPTreeNode, key int, item *CacheItem) (int, *BPTreeNode) {
//	if node.isLeaf {
//		i := sort.SearchInts(node.keys, key)
//		if i < len(node.keys) && node.keys[i] == key {
//			node.values[i] = item
//			return 0, nil
//		}
//		node.keys = append(node.keys, 0)
//		copy(node.keys[i+1:], node.keys[i:])
//		node.keys[i] = key
//		node.values = append(node.values, nil)
//		copy(node.values[i+1:], node.values[i:])
//		node.values[i] = item
//		if len(node.keys) < treeOrder {
//			return 0, nil
//		}
//		return t.splitLeaf(node)
//	}
//	i := sort.Search(len(node.keys), func(i int) bool {
//		return key < node.keys[i]
//	})
//	newKey, newChild := t.insert(node.children[i], key, item)
//	if newChild == nil {
//		return 0, nil
//	}
//	node.keys = append(node.keys, 0)
//	copy(node.keys[i+1:], node.keys[i:])
//	node.keys[i] = newKey
//	node.children = append(node.children, nil)
//	copy(node.children[i+2:], node.children[i+1:])
//	node.children[i+1] = newChild
//	if len(node.keys) < treeOrder {
//		return 0, nil
//	}
//	return t.splitInternal(node)
//}
//
//func (t *BPTree) splitLeaf(node *BPTreeNode) (int, *BPTreeNode) {
//	splitIndex := len(node.keys) / 2
//	newNode := &BPTreeNode{
//		isLeaf: true,
//		keys:   append([]int(nil), node.keys[splitIndex:]...),
//		values: append([]*CacheItem(nil), node.values[splitIndex:]...),
//		next:   node.next,
//	}
//	node.keys = node.keys[:splitIndex]
//	node.values = node.values[:splitIndex]
//	node.next = newNode
//	return newNode.keys[0], newNode
//}
//
//func (t *BPTree) splitInternal(node *BPTreeNode) (int, *BPTreeNode) {
//	splitIndex := len(node.keys) / 2
//	promoted := node.keys[splitIndex]
//	newNode := &BPTreeNode{
//		isLeaf:   false,
//		keys:     append([]int(nil), node.keys[splitIndex+1:]...),
//		children: append([]*BPTreeNode(nil), node.children[splitIndex+1:]...),
//	}
//	node.keys = node.keys[:splitIndex]
//	node.children = node.children[:splitIndex+1]
//	return promoted, newNode
//}
//
//func (t *BPTree) Delete(key int) {
//	t.mu.Lock()
//	defer t.mu.Unlock()
//	t.deleteRecursive(nil, t.root, key, -1)
//	if !t.root.isLeaf && len(t.root.children) == 1 {
//		t.root = t.root.children[0]
//	}
//}
//
//func (t *BPTree) deleteRecursive(parent *BPTreeNode, node *BPTreeNode, key int, childIndex int) bool {
//	minKeysLeaf := (treeOrder + 1) / 2
//	minKeysInternal := (treeOrder+1)/2 - 1
//	if node.isLeaf {
//		i := sort.SearchInts(node.keys, key)
//		if i >= len(node.keys) || node.keys[i] != key {
//			return false
//		}
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
//	i := 0
//	for i < len(node.keys) && key >= node.keys[i] {
//		i++
//	}
//	underflow := t.deleteRecursive(node, node.children[i], key, i)
//	if !underflow {
//		return false
//	}
//	return t.rebalanceInternal(node, i, minKeysInternal)
//}
//
//func (t *BPTree) rebalanceLeaf(parent, leaf *BPTreeNode, idx, minLeaf int) bool {
//	if idx > 0 {
//		leftSibling := parent.children[idx-1]
//		if len(leftSibling.keys) > minLeaf {
//			borrowKey := leftSibling.keys[len(leftSibling.keys)-1]
//			borrowValue := leftSibling.values[len(leftSibling.values)-1]
//			leftSibling.keys = leftSibling.keys[:len(leftSibling.keys)-1]
//			leftSibling.values = leftSibling.values[:len(leftSibling.values)-1]
//			leaf.keys = append([]int{borrowKey}, leaf.keys...)
//			leaf.values = append([]*CacheItem{borrowValue}, leaf.values...)
//			parent.keys[idx-1] = leaf.keys[0]
//			return false
//		}
//	}
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
//	if idx > 0 {
//		leftSibling := parent.children[idx-1]
//		leftSibling.keys = append(leftSibling.keys, leaf.keys...)
//		leftSibling.values = append(leftSibling.values, leaf.values...)
//		leftSibling.next = leaf.next
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
//func (t *BPTree) rebalanceInternal(parent *BPTreeNode, idx, minInternal int) bool {
//	child := parent.children[idx]
//	if idx > 0 {
//		leftSibling := parent.children[idx-1]
//		if len(leftSibling.keys) > minInternal {
//			separator := parent.keys[idx-1]
//			borrowKey := leftSibling.keys[len(leftSibling.keys)-1]
//			borrowChild := leftSibling.children[len(leftSibling.children)-1]
//			leftSibling.keys = leftSibling.keys[:len(leftSibling.keys)-1]
//			leftSibling.children = leftSibling.children[:len(leftSibling.children)-1]
//			child.keys = append([]int{separator}, child.keys...)
//			child.children = append([]*BPTreeNode{borrowChild}, child.children...)
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
//type CacheItem struct {
//	key        int
//	value      GenericRecord
//	dirty      bool
//	lastAccess time.Time
//	index      int
//}
//
//type AccessHeap []*CacheItem
//
//func (h AccessHeap) Len() int           { return len(h) }
//func (h AccessHeap) Less(i, j int) bool { return h[i].lastAccess.Before(h[j].lastAccess) }
//func (h AccessHeap) Swap(i, j int) {
//	h[i], h[j] = h[j], h[i]
//	h[i].index = i
//	h[j].index = j
//}
//
//func (h *AccessHeap) Push(x any) {
//	item := x.(*CacheItem)
//	item.index = len(*h)
//	*h = append(*h, item)
//}
//
//func (h *AccessHeap) Pop() any {
//	old := *h
//	n := len(old)
//	item := old[n-1]
//	item.index = -1
//	*h = old[0 : n-1]
//	return item
//}
//
//func (h *AccessHeap) update(item *CacheItem, lastAccess time.Time) {
//	item.lastAccess = lastAccess
//	heap.Fix(h, item.index)
//}
//
//type PersistentLRU struct {
//	capacity int
//	storeDir string
//	mu       sync.Mutex
//	tree     *BPTree
//	heap     AccessHeap
//	size     int
//}
//
//func NewPersistentLRU(capacity int, storeDir string) (*PersistentLRU, error) {
//	if err := os.MkdirAll(storeDir, os.ModePerm); err != nil {
//		return nil, fmt.Errorf("cannot create storage directory: %w", err)
//	}
//	return &PersistentLRU{
//		capacity: capacity,
//		storeDir: storeDir,
//		tree:     NewBPTree(),
//		heap:     make(AccessHeap, 0, capacity),
//	}, nil
//}
//
//func (lru *PersistentLRU) fileName(key int) string {
//	return filepath.Join(lru.storeDir, fmt.Sprintf("doc_%d.json", key))
//}
//
//func (lru *PersistentLRU) persistToDisk(key int, rec GenericRecord) error {
//	data, err := json.Marshal(rec)
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
//func (lru *PersistentLRU) loadFromDisk(key int) (GenericRecord, error) {
//	data, err := os.ReadFile(lru.fileName(key))
//	if err != nil {
//		return nil, fmt.Errorf("read error: %w", err)
//	}
//	var rec GenericRecord
//	if err := json.Unmarshal(data, &rec); err != nil {
//		return nil, fmt.Errorf("unmarshal error: %w", err)
//	}
//	return rec, nil
//}
//
//func (lru *PersistentLRU) updateItemUsage(item *CacheItem) {
//	now := time.Now()
//	item.lastAccess = now
//	heap.Fix(&lru.heap, item.index)
//}
//
//func (lru *PersistentLRU) evictIfNeeded() {
//	for lru.size > lru.capacity {
//		item := heap.Pop(&lru.heap).(*CacheItem)
//		lru.tree.Delete(item.key)
//		lru.size--
//		if item.dirty {
//			if err := lru.persistToDisk(item.key, item.value); err != nil {
//				log.Printf("flush eviction key %d: %v", item.key, err)
//			}
//		}
//		log.Printf("Evicted key %d from cache", item.key)
//	}
//}
//
//func (lru *PersistentLRU) Get(key int) (GenericRecord, error) {
//	lru.mu.Lock()
//	defer lru.mu.Unlock()
//	item := lru.tree.Search(key)
//	if item != nil {
//		lru.updateItemUsage(item)
//		return item.value, nil
//	}
//	rec, err := lru.loadFromDisk(key)
//	if err != nil {
//		return nil, err
//	}
//	newItem := &CacheItem{
//		key:        key,
//		value:      rec,
//		dirty:      false,
//		lastAccess: time.Now(),
//	}
//	lru.tree.Insert(key, newItem)
//	heap.Push(&lru.heap, newItem)
//	lru.size++
//	lru.evictIfNeeded()
//	return rec, nil
//}
//
//func (lru *PersistentLRU) Set(key int, rec GenericRecord) error {
//	lru.mu.Lock()
//	defer lru.mu.Unlock()
//	now := time.Now()
//	item := lru.tree.Search(key)
//	if item != nil {
//		item.value = rec
//		item.dirty = true
//		item.lastAccess = now
//		heap.Fix(&lru.heap, item.index)
//	} else {
//		newItem := &CacheItem{
//			key:        key,
//			value:      rec,
//			dirty:      true,
//			lastAccess: now,
//		}
//		lru.tree.Insert(key, newItem)
//		heap.Push(&lru.heap, newItem)
//		lru.size++
//		lru.evictIfNeeded()
//	}
//	return lru.persistToDisk(key, rec)
//}
//
//func (lru *PersistentLRU) Delete(key int) error {
//	lru.mu.Lock()
//	defer lru.mu.Unlock()
//	lru.tree.Delete(key)
//	for i, it := range lru.heap {
//		if it.key == key {
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
//func (lru *PersistentLRU) flushDirty() {
//	lru.mu.Lock()
//	defer lru.mu.Unlock()
//	for _, item := range lru.heap {
//		if item.dirty {
//			if err := lru.persistToDisk(item.key, item.value); err != nil {
//				log.Printf("flush error key %d: %v", item.key, err)
//			} else {
//				item.dirty = false
//			}
//		}
//	}
//}
//
//func (lru *PersistentLRU) logUsage() {
//	lru.mu.Lock()
//	defer lru.mu.Unlock()
//	log.Printf("[Cache] %d items in memory (capacity: %d)", lru.size, lru.capacity)
//}
//
//func (lru *PersistentLRU) StartBackgroundFlush(ctx context.Context, interval time.Duration) {
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
//func main() {
//	storeDir := "./storage"
//	cache, err := NewPersistentLRU(3, storeDir)
//	if err != nil {
//		log.Fatalf("Error initializing PersistentLRU: %v", err)
//	}
//	ctx, cancel := context.WithCancel(context.Background())
//	defer cancel()
//	cache.StartBackgroundFlush(ctx, 10*time.Second)
//	for i := 1; i <= 5; i++ {
//		rec := GenericRecord{
//			"id":      i,
//			"content": fmt.Sprintf("This is document number %d", i),
//			"meta":    fmt.Sprintf("metadata-%d", i),
//		}
//		if err := cache.Set(i, rec); err != nil {
//			log.Printf("Error setting key %d: %v", i, err)
//		} else {
//			log.Printf("Inserted key %d", i)
//		}
//	}
//	for i := 3; i <= 5; i++ {
//		if rec, err := cache.Get(i); err != nil {
//			log.Printf("Error getting key %d: %v", i, err)
//		} else {
//			log.Printf("Accessed key %d: %v", i, rec)
//		}
//	}
//	log.Println("Waiting for background flush...")
//	cache.logUsage()
//	log.Println("Demo complete.")
//	time.Sleep(1 * time.Minute)
//}
