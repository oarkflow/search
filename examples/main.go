package main

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/oarkflow/search"
	"github.com/oarkflow/search/lib"
)

func main() {
	bt := []byte(`{"filters":[],"query":"ONDANSETRON","condition":"","boolMode":"AND","exact":false,"tolerance":0,"relevance":{"k":0,"b":0,"d":0},"paginate":true,"offset":0,"limit":20,"lang":""}`)
	var params search.Params
	json.Unmarshal(bt, &params)
	icds := lib.ReadFileAsMap("sample.json")
	db, _ := search.New[map[string]any]()
	var startTime = time.Now()
	before := lib.Stats()
	db.InsertWithPool(icds, 3, 100)
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
