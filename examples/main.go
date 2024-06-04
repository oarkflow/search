package main

import (
	"fmt"
	"time"

	"github.com/oarkflow/filters"

	"github.com/oarkflow/search"
	"github.com/oarkflow/search/lib"
	"github.com/oarkflow/search/tokenizer"
)

func main() {
	icds := lib.ReadFileAsMap("sample.json")
	db, _ := search.New[map[string]any](&search.Config{
		Storage:         "memory",
		DefaultLanguage: tokenizer.ENGLISH,
		TokenizerConfig: &tokenizer.Config{
			EnableStemming:  true,
			EnableStopWords: true,
		},
		IndexKeys: search.DocFields(icds[0]),
	})
	var startTime = time.Now()
	before := lib.Stats()
	db.InsertWithPool(icds, 3, 100)
	after := lib.Stats()
	fmt.Println(fmt.Sprintf("Usage: %dMB; Before: %dMB; After: %dMB", after-before, before, after))
	fmt.Println("Total Documents", db.DocumentLen())
	fmt.Println("Indexing took", time.Since(startTime))
	startTime = time.Now()
	s, err := db.Search(&search.Params{
		Filters: []filters.Filter{
			{
				Field:    "work_item_id",
				Operator: filters.Equal,
				Value:    "55",
			},
		},
	})
	if err != nil {
		panic(err)
	}
	fmt.Println("Searching took", time.Since(startTime))
	fmt.Println(s.Hits)
}
