package main

import (
	"fmt"
	"time"

	"github.com/oarkflow/search"
	"github.com/oarkflow/search/convert"
	"github.com/oarkflow/search/filters"
	"github.com/oarkflow/search/lib"
	"github.com/oarkflow/search/tokenizer"
)

func main() {
	// Example usage
	val := any("123")

	intVal, ok := convert.To(0, val)
	if ok {
		fmt.Printf("Converted to int: %d\n", intVal)
	} else {
		fmt.Println("Failed to convert to int")
	}

	// Example usage of slice conversion
	valSlice := any([]any{"1", "2", "3"})
	intSlice, ok := convert.To([]int{}, valSlice)
	if ok {
		fmt.Printf("Converted to []int: %v\n", intSlice)
	} else {
		fmt.Println("Failed to convert to []int")
	}
}

func mai1n() {
	icds := lib.ReadFileAsMap("cpt_codes.json")
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
		Query: "Amput",
		Filters: []filters.Filter{
			{
				Field:    "effective_date",
				Operator: filters.Between,
				Value:    []string{"2015-01-01", "2015-05-01"},
			},
		},
	})
	if err != nil {
		panic(err)
	}
	fmt.Println("Searching took", time.Since(startTime))
	fmt.Println(s.Hits)
}
