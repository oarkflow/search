package main

import (
	"context"
	"fmt"
	"log"
	"time"

	v1 "github.com/oarkflow/search/v1"
)

func main() {
	ctx := context.Background()
	jsonFilePath := "charge_master.json"
	startTime := time.Now()
	index := v1.NewIndex("test")
	err := index.Build(ctx, jsonFilePath)
	if err != nil {
		log.Fatalf("Error building index: %v", err)
	}
	fmt.Println(fmt.Sprintf("Index built for %d documents in %s", index.TotalDocs, time.Since(startTime)))
	termQ := v1.NewTermQuery("33965", true, 1)
	boolQ := v1.BoolQuery{
		Must: []v1.Query{termQ},
	}
	startTime = time.Now()
	scoredDocs, err := index.Search(ctx, boolQ, "33965")
	if err != nil {
		log.Fatalf("Error searching index: %v", err)
	}
	since := time.Since(startTime)
	page := 1
	perPage := 1
	paginatedResults := v1.Paginate(scoredDocs, page, perPage)
	fmt.Println(fmt.Sprintf("Found %d matching documents (showing page %d): Latency: %s", len(scoredDocs), page, since))
	for _, sd := range paginatedResults {
		rec := index.Documents[sd.DocID]
		fmt.Println(fmt.Sprintf("DocID: %d | Score: %.4f | Data: %+v", sd.DocID, sd.Score, rec))
	}
}
