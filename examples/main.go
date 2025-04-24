package main

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/oarkflow/search"
	"github.com/oarkflow/search/lib"
)

func main() {
	bt := []byte(`{"sort_field": "display_name"}`)
	var params search.Params
	json.Unmarshal(bt, &params)
	icds := lib.ReadFileAsMap("charge_master.json")
	db, _ := search.New[map[string]any](&search.Config{Storage: "memory"})
	var startTime = time.Now()
	before := lib.Stats()
	for _, icd := range icds {
		db.Insert(icd)
	}
	// db.InsertWithPool(icds, 3, 100)
	after := lib.Stats()
	fmt.Println(fmt.Sprintf("Usage: %dMB; Before: %dMB; After: %dMB", after-before, before, after))
	fmt.Println("Total Documents", db.DocumentLen())
	fmt.Println("Indexing took", time.Since(startTime))
	startTime = time.Now()
	s, err := db.Search(&params)
	if err != nil {
		panic(err)
	}
	fmt.Println("Searching took", time.Since(startTime), s.Message)
	fmt.Println(s.Total, s.Hits, s.Count)
}
