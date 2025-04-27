package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/oarkflow/search/lib"
	v1 "github.com/oarkflow/search/v1"
)

func main() {
	ctx := context.Background()
	jsonFilePath := "charge_master.json"
	startTime := time.Now()
	index := v1.NewIndex("test")
	before := lib.Stats()
	err := index.Build(ctx, jsonFilePath)
	after := lib.Stats()
	fmt.Println(fmt.Sprintf("Usage: %dMB; Before: %dMB; After: %dMB", after-before, before, after))
	if err != nil {
		log.Fatalf("Error building index: %v", err)
	}
	fmt.Println(fmt.Sprintf("Index built for %d documents in %s", index.TotalDocs, time.Since(startTime)))
	termQ := v1.NewTermQuery("33965", true, 1)
	boolQ := v1.BoolQuery{
		Must: []v1.Query{termQ},
	}
	startTime = time.Now()
	params := v1.SearchParams{
		Page:    1,
		PerPage: 20,
		SortFields: []v1.SortField{
			{
				Field:     "charge_type",
				Ascending: true,
			},
		},
	}
	before = lib.Stats()
	scoredDocs, err := index.Search(ctx, boolQ, params)
	if err != nil {
		log.Fatalf("Error searching index: %v", err)
	}
	after = lib.Stats()
	fmt.Println(fmt.Sprintf("Usage: %dMB; Before: %dMB; After: %dMB", after-before, before, after))
	since := time.Since(startTime)
	fmt.Println(fmt.Sprintf("Found %d matching documents (showing page %d): Latency: %s", len(scoredDocs.Results), scoredDocs.Page, since))
	for _, sd := range scoredDocs.Results {
		rec, _ := index.GetDocument(sd.DocID)
		fmt.Println(fmt.Sprintf("DocID: %d | Score: %.4f | Data: %+v", sd.DocID, sd.Score, rec))
	}
}
