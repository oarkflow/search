package main

import (
	"fmt"
	"log"
	"time"

	v1 "github.com/oarkflow/search/v1"
)

func main() {
	jsonFilePath := "charge_master.json"
	startTime := time.Now()
	index := v1.NewIndex()
	err := index.Build(jsonFilePath)
	if err != nil {
		log.Fatalf("Error building index: %v", err)
	}
	fmt.Println(fmt.Sprintf("Index built for %d documents in %s", index.TotalDocs, time.Since(startTime)))
	termQ := v1.NewTermQuery("33965", true, 1)
	boolQ := v1.BoolQuery{
		Must: []v1.Query{termQ},
	}
	startTime = time.Now()
	scoredDocs := index.Search(boolQ, "33965")
	since := time.Since(startTime)
	page := 1
	perPage := 1
	paginatedResults := v1.Paginate(scoredDocs, page, perPage)
	fmt.Println(fmt.Sprintf("Found %d matching documents (showing page %d): Latency: %s", len(scoredDocs), page, since))
	for _, sd := range paginatedResults {
		rec := index.Documents[sd.DocID]
		fmt.Println(fmt.Sprintf("DocID: %d | Score: %.4f | Data: %+v", sd.DocID, sd.Score, rec))
	}

	termQ = v1.NewTermQuery("33964", true, 1)
	startTime = time.Now()
	scoredDocs = index.Search(boolQ, "33964")
	since = time.Since(startTime)
	paginatedResults = v1.Paginate(scoredDocs, page, perPage)
	fmt.Println(fmt.Sprintf("Found %d matching documents (showing page %d): Latency: %s", len(scoredDocs), page, since))
	for _, sd := range paginatedResults {
		rec := index.Documents[sd.DocID]
		fmt.Println(fmt.Sprintf("DocID: %d | Score: %.4f | Data: %+v", sd.DocID, sd.Score, rec))
	}
}
