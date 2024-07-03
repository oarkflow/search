package main

import (
	"fmt"
	"time"

	"github.com/oarkflow/search"
	"github.com/oarkflow/search/lib"
)

func main() {
	icds := lib.ReadFileAsMap("sample.json")
	db, _ := search.New[map[string]any, int8]()
	var startTime = time.Now()
	before := lib.Stats()
	errs := db.InsertWithPool(icds, 3, 100)
	if len(errs) > 0 {
		panic(errs[0])
	}
	after := lib.Stats()
	fmt.Println(fmt.Sprintf("Usage: %dMB; Before: %dMB; After: %dMB", after-before, before, after))
	fmt.Println("Total Documents", db.DocumentLen())
	fmt.Println("Indexing took", time.Since(startTime))
	startTime = time.Now()
	s, err := db.Search(&search.Params{
		Condition: "effective_date LIKE '2015-01-01%'",
	})
	if err != nil {
		panic(err)
	}
	fmt.Println("Searching took", time.Since(startTime), s.Message)
	fmt.Println(s.Hits)
}
