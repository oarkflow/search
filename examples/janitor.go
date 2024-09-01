package main

import (
	"fmt"
	"time"

	"github.com/oarkflow/search/janitor"
	"github.com/oarkflow/search/storage"
	"github.com/oarkflow/search/storage/memdb"
)

func main() {
	targetCache, _ := memdb.New[string, string](100, storage.StringComparator)
	sourceCache := janitor.NewLRUCache[string, string](3)
	cleaner := janitor.New[string, string](sourceCache, targetCache, 5*time.Second, true)
	go cleaner.CleanUp()
	sourceCache.Set("key1", "value1")
	sourceCache.Set("key2", "value2")
	sourceCache.Set("key3", "value3")
	if val, found := cleaner.Get("key1"); found {
		fmt.Println("Found in cache:", val)
	} else {
		fmt.Println("Not found")
	}
	sourceCache.Set("key4", "value4")
	sourceCache.Set("key5", "value5")
	time.Sleep(10 * time.Second)
	if val, found := targetCache.Get("key2"); found {
		fmt.Println("Evicted key found in target cache:", val)
	} else {
		fmt.Println("Evicted key not found in target")
	}
	if val, found := cleaner.Get("key2"); found {
		fmt.Println("Evicted key found in source or target cache:", val)
	} else {
		fmt.Println("Evicted key not found in source or target")
	}
	cleaner.Stop()
}
