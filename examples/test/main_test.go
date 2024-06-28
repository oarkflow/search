package main

import (
	"fmt"
	"testing"
	"time"

	"github.com/oarkflow/search"
	"github.com/oarkflow/search/lib"
)

func TestMap(t *testing.T) {
	icds := lib.ReadFileAsMap("icd10_codes.json")
	db, _ := search.New[map[string]any]()
	var startTime = time.Now()
	before := lib.Stats()
	db.InsertWithPool(icds, 3, 100)
	after := lib.Stats()
	fmt.Println(fmt.Sprintf("Usage: %dMB; Before: %dMB; After: %dMB", after-before, before, after))
	fmt.Println("Total Documents", db.DocumentLen())
	fmt.Println("Indexing took", time.Since(startTime))
	startTime = time.Now()
	s, err := db.Search(&search.Params{
		Query: "presence",
	})
	if err != nil {
		panic(err)
	}
	fmt.Println("Searching took", time.Since(startTime))
	fmt.Println(len(s.Hits))
}
