package main

import (
	"fmt"
	"math/rand"
	"runtime"
	"time"

	"github.com/oarkflow/maps"
)

const (
	numElements = 1e6
)

type KeyValue struct {
	Key   int64
	Value float64
}

type HashTable struct {
	size    int
	buckets [][]KeyValue
}

func NewHashTable(size int) *HashTable {
	return &HashTable{
		size:    size,
		buckets: make([][]KeyValue, size),
	}
}

func (ht *HashTable) hash(key int64) int {
	return int(key % int64(ht.size))
}

func (ht *HashTable) Insert(key int64, value float64) {
	index := ht.hash(key)
	for i, kv := range ht.buckets[index] {
		if kv.Key == key {
			ht.buckets[index][i].Value = value
			return
		}
	}
	ht.buckets[index] = append(ht.buckets[index], KeyValue{Key: key, Value: value})
}

func (ht *HashTable) Get(key int64) (float64, bool) {
	index := ht.hash(key)
	for _, kv := range ht.buckets[index] {
		if kv.Key == key {
			return kv.Value, true
		}
	}
	return 0, false
}

func main() {
	rand.Seed(time.Now().UnixNano())

	// Test Go's built-in map
	fmt.Println("Testing built-in map")
	m := make(map[int64]float64, numElements)
	for i := int64(0); i < numElements; i++ {
		m[i] = rand.Float64()
	}

	var mStats runtime.MemStats
	runtime.ReadMemStats(&mStats)
	fmt.Printf("Built-in map memory usage: %d bytes\n", mStats.Alloc)

	// Test custom hash table
	fmt.Println("Testing custom hash table")
	ht := NewHashTable(numElements)
	for i := int64(0); i < numElements; i++ {
		ht.Insert(i, rand.Float64())
	}

	var htStats runtime.MemStats
	runtime.ReadMemStats(&htStats)
	fmt.Printf("Custom hash table memory usage: %d bytes\n", htStats.Alloc-mStats.Alloc)

	// Test custom hash table
	fmt.Println("Testing maps.IMap")
	ma := maps.New[int64, float64](numElements)
	for i := int64(0); i < numElements; i++ {
		ma.Set(i, rand.Float64())
	}

	var maStats runtime.MemStats
	runtime.ReadMemStats(&maStats)
	fmt.Printf("maps.Imap memory usage: %d bytes\n", maStats.Alloc-htStats.Alloc)
}
