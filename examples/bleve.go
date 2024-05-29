package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"runtime"
	"time"

	"github.com/blevesearch/bleve/v2"
	"github.com/oarkflow/xid"
)

func stats() uint64 {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.Alloc / (1024 * 1024)
}

func main() {
	os.RemoveAll("example.bleve")
	data := readFileAsMap()
	mapping := bleve.NewIndexMapping()
	idx, err := bleve.New("example.bleve", mapping)
	if err != nil {
		panic(err)
	}
	before := stats()
	batch := idx.NewBatch()
	var startTime = time.Now()
	fmt.Println("started indexing", startTime)
	for i, d := range data {
		if err = batch.Index(xid.New().String(), d); err != nil {
			panic(err)
		}
		if i%1000 == 0 && batch.Size() > 0 {
			if err = idx.Batch(batch); err != nil {
				panic(err)
			}
			batch.Reset()
		}
		if err = batch.Index(xid.New().String(), d); err != nil {
			panic(err)
		}
	}
	if batch.Size() > 0 {
		if err = idx.Batch(batch); err != nil {
			panic(err)
		}
	}
	after := stats()
	fmt.Println(fmt.Sprintf("Usage: %dMB; Before: %dMB; After: %dMB", after-before, before, after))
	idx.Close()
	fmt.Println("Indexing took", time.Since(startTime))
	index2, err := bleve.Open("example.bleve")
	if err != nil {
		panic(err)
	}
	startTime = time.Now()
	// Create individual field queries
	query := bleve.NewMatchQuery("presence")

	companyQuery := bleve.NewMatchQuery("Z9710")
	companyQuery.SetField("code")

	// Combine them into a single compound query
	compoundQuery := bleve.NewConjunctionQuery(query, companyQuery)

	// Search
	searchRequest := bleve.NewSearchRequest(compoundQuery)
	searchResult, err := index2.Search(searchRequest)
	if err != nil {
		log.Fatalf("Error executing search: %v", err)
	}
	/*query := bleve.NewQueryStringQuery("unspecified")
	searchRequest := bleve.NewSearchRequest(query)
	searchResult, _ := index2.Search(searchRequest)*/
	fmt.Println("Searching took", time.Since(startTime))
	fmt.Println(searchResult.Cost)
}

func readFileAsMap() []map[string]any {
	var d []map[string]any
	data, err := os.Open("./icd10_codes.json")
	if err != nil {
		panic(err)
	}
	decoder := json.NewDecoder(data)
	err = decoder.Decode(&d)
	if err != nil {
		panic(err)
	}
	return d
}
